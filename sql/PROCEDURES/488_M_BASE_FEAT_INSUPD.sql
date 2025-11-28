-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_FEAT_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
DECLARE
       run_id STRING;
       PRCS_ID int;
	   v_start_time TIMESTAMP;
BEGIN
       run_id := (SELECT run_id FROM control_run_id WHERE worklet_name = :worklet_name ORDER BY insert_ts DESC LIMIT 1);
       PRCS_ID := 1; --(SELECT param_value FROM control_params WHERE run_id = :run_id AND param_name = ''PRCS_ID'' LIMIT 1);
	   v_start_time := CURRENT_TIMESTAMP();

-- Component LKP_TERADATA_ETL_REF_XLAT_FEAT_CLASFCN_CD, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_FEAT_CLASFCN_CD AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''FEAT_CLASFCN_TYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_FEAT_COVRAGE_CD, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_FEAT_COVRAGE_CD AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''INSRNC_CVGE_TYPE'' 

         		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''GW'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_FEAT_INSRNC_SBTYPE_CD, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_FEAT_INSRNC_SBTYPE_CD AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''FEAT_INSRNC_SBTYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM in (''pc_etlclausepattern.clausetype'',''derived'',''pctl_documenttype.typecode'')

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS in (''GW'',''DS'') 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_FEAT_SBTYPE_CD, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_FEAT_SBTYPE_CD AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''FEAT_SBTYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_INSRNC_LOB_TYPE_CD, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_INSRNC_LOB_TYPE_CD AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''INSRNC_LOB_TYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component SQ_feat, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_feat AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as FEAT_SBTYPE_CD,
$2 as FEAT_INSRNC_SBTYPE_CD,
$3 as FEAT_CLASFCN_CD,
$4 as FEAT_DESC,
$5 as FEAT_NAME,
$6 as COMN_FEAT_NAME,
$7 as FEAT_LVL_SBTYPE_CNT,
$8 as INSRNC_LOB_TYPE_CD,
$9 as FEAT_COVERABLE_TYPE_TXT,
$10 as NK_SRC_KEY,
$11 as FEAT_DATA_TYPE_NAME,
$12 as FEAT_DTL_MODL_TYPE_NAME,
$13 as FEAT_DTL_CD_NAME,
$14 as FEAT_DTL_VAL,
$15 as FEAT_DTL_VAL_TYPE,
$16 as FEAT_DTL_COL_NAME,
$17 as INSRNC_CVGE_TYPE_CD,
$18 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select * from 

/* select distinct rank() over (partition by FEAT_SBTYPE_CD, NK_SRC_KEY order by FEAT_NAME) rk, tmp.* from */
(

select

distinct

cast (''FEAT_SBTYPE11'' as varchar (255)) as FEAT_SBTYPE_CD, /* Modifier */
cast ('' ''  as varchar (255)) as FEAT_INSRNC_SBTYPE_CD,

cast ('' ''  as varchar (255))  as FEAT_CLASFCN_CD,

cast (pc_etlmodifierpattern.name_stg as varchar (255)) as FEAT_DESC,

cast (pc_etlmodifierpattern.name_stg as varchar (255)) as FEAT_NAME,

cast (pc_etlmodifierpattern.name_stg as varchar (255)) as COMN_FEAT_NAME,

cast (''1'' as varchar (255)) as FEAT_LVL_SBTYPE_CNT,

cast(case

when pc_etlmodifierpattern.OwningEntityType_alfa_stg in (''PersonalAutoLine'',''PersonalVehicle'',''PAWatercraftTrailer_alfa'',''PAWatercraftMotor_alfa'') then ''INSRNC_LOB_TYPE9''

when  pc_etlmodifierpattern.OwningEntityType_alfa_stg in (''HomeownersLine_HOE'',''HOLineSchCovItem_alfa'',''Dwelling_HOE'',''HOLineSchExclItem_alfa'') then ''INSRNC_LOB_TYPE8''

when pc_etlmodifierpattern.OwningEntityType_alfa_stg in (''BP7BldgSchedCovItem'', ''BP7Building'', ''BP7BusinessOwnersLine'', ''BP7Classification'', ''BP7LineSchedCovItem'', ''BP7Location'', ''BP7LocSchedCovItem'') then ''INSRNC_LOB_TYPE10''

when pc_etlmodifierpattern.OwningEntityType_alfa_stg in (''PUPPersonalUmbrellaLine'',''PUPPersonalUmbrellaLineScheduleExclItem'') then ''INSRNC_LOB_TYPE11''

when pc_etlmodifierpattern.OwningEntityType_alfa_stg  in (''FOPBlanket'',''FOPDwelling'',''FOPDwellingScheduleCovItem'',''FOPDwellingScheduleExclItem'',

''FOPFarmownersLineScheduleCovItem'',''FOPFeedAndSeed'',''FOPLiability'',''FOPLiabilityScheduleCovItem'',''FOPLiabilityScheduleExclItem'',''FOPLivestock'',

''FOPMachinery'',''FOPOutbuilding'',''FOPFarmownersLine'')then ''INSRNC_LOB_TYPE_12''

else '' ''

end as varchar(255)) INSRNC_LOB_TYPE_CD ,

cast (coalesce(pc_etlmodifierpattern.OwningEntityType_alfa_stg, '' '') as varchar (255)) as FEAT_COVERABLE_TYPE_TXT,

cast (pc_etlmodifierpattern.patternid_stg as varchar (255)) as NK_SRC_KEY,

cast ('' ''  as varchar (255)) as FEAT_DATA_TYPE_NAME,

cast ('' ''  as varchar (255)) as FEAT_DTL_MODL_TYPE_NAME,

cast ('' ''  as varchar (255)) as FEAT_DTL_CD_NAME,

cast ('' ''  as varchar (255))  as FEAT_DTL_VAL,

cast ('' ''  as varchar (255)) as FEAT_DTL_VAL_TYPE,

cast ('' ''  as varchar (255)) as FEAT_DTL_COL_NAME,

cast ('' ''  as varchar (255)) as INSRNC_CVGE_TYPE_CD

FROM DB_T_PROD_STAG.pc_etlmodifierpattern pc_etlmodifierpattern



/* where  OwningEntityType_alfa in (''PersonalAutoLine'',''PersonalVehicle'',''PAWatercraftTrailer_alfa'',''PAWatercraftMotor_alfa'',''HomeownersLine_HOE'',''HOLineSchCovItem_alfa'',''Dwelling_HOE'',''HOLineSchExclItem_alfa'') */


union





select



''FEAT_SBTYPE7'' as FEAT_SBTYPE_CD, /* Clause */
pc_etlclausepattern.clausetype_stg as FEAT_INSRNC_SBTYPE_CD,

'' '' as FEAT_CLASFCN_CD,

pc_etlclausepattern.name_stg as FEAT_DESC,

pc_etlclausepattern.name_stg as FEAT_NAME,

pc_etlclausepattern.name_stg as COMN_FEAT_NAME,

''1'' as FEAT_LVL_SBTYPE_CNT,

case

when pc_etlclausepattern.OwningEntityType_stg in (''PersonalAutoLine'',''PersonalVehicle'',''PAWatercraftTrailer_alfa'',''PAWatercraftMotor_alfa'') then ''INSRNC_LOB_TYPE9''

when  pc_etlclausepattern.OwningEntityType_stg in (''HomeownersLine_HOE'',''HOLineSchCovItem_alfa'',''Dwelling_HOE'',''HOLineSchExclItem_alfa'') then ''INSRNC_LOB_TYPE8''

when pc_etlclausepattern.OwningEntityType_stg in (''BP7BldgSchedCovItem'', ''BP7Building'', ''BP7BusinessOwnersLine'', ''BP7Classification'', ''BP7LineSchedCovItem'', ''BP7Location'', ''BP7LocSchedCovItem'') then ''INSRNC_LOB_TYPE10''

when pc_etlclausepattern.OwningEntityType_stg  in (''PUPPersonalUmbrellaLine'',''PUPPersonalUmbrellaLineScheduleExclItem'') then ''INSRNC_LOB_TYPE11''

when pc_etlclausepattern.OwningEntityType_stg  in (''FOPBlanket'',''FOPDwelling'',''FOPDwellingScheduleCovItem'',''FOPDwellingScheduleExclItem'',

''FOPFarmownersLineScheduleCovItem'',''FOPFeedAndSeed'',''FOPLiability'',''FOPLiabilityScheduleCovItem'',''FOPLiabilityScheduleExclItem'',''FOPLivestock'',

''FOPMachinery'',''FOPOutbuilding'',''FOPFarmownersLine'')then ''INSRNC_LOB_TYPE_12''

else '' ''

end INSRNC_LOB_TYPE_CD ,

pc_etlclausepattern.OwningEntityType_stg as FEAT_COVERABLE_TYPE_TXT,

pc_etlclausepattern.patternid_stg as NK_SRC_KEY,

'' '' as FEAT_DATA_TYPE_NAME,

'' '' as FEAT_DTL_MODL_TYPE_NAME,

'' '' as FEAT_DTL_CD_NAME,

'' '' as FEAT_DTL_VAL,

'' '' as FEAT_DTL_VAL_TYPE,

'' '' as FEAT_DTL_COL_NAME,

cast (pc_etlclausepattern.name_stg  as varchar (255)) as INSRNC_CVGE_TYPE_CD

FROM DB_T_PROD_STAG.pc_etlclausepattern pc_etlclausepattern

/* where  OwningEntityType in (''PersonalAutoLine'',''PersonalVehicle'',''PAWatercraftTrailer_alfa'',''PAWatercraftMotor_alfa'',''HomeownersLine_HOE'',''HOLineSchCovItem_alfa'',''Dwelling_HOE'',''HOLineSchExclItem_alfa'') */


union 



select



''FEAT_SBTYPE6'' as FEAT_SBTYPE_CD, /* Term */
pc_etlclausepattern.clausetype_stg as FEAT_INSRNC_SBTYPE_CD,

'' '' as FEAT_CLASFCN_CD,

(pc_etlcovtermpattern.name_stg || ''-''  || pc_etlcovtermpattern.modeltype_stg) as FEAT_DESC,

pc_etlcovtermpattern.name_stg  as FEAT_NAME,

(pc_etlcovtermpattern.name_stg || ''-'' || pc_etlcovtermpattern.modeltype_stg) as COMN_FEAT_NAME,

''2'' as FEAT_LVL_SBTYPE_CNT,

case

when pc_etlclausepattern.OwningEntityType_stg in (''PersonalAutoLine'',''PersonalVehicle'',''PAWatercraftTrailer_alfa'',''PAWatercraftMotor_alfa'') then ''INSRNC_LOB_TYPE9''

when  pc_etlclausepattern.OwningEntityType_stg in (''HomeownersLine_HOE'',''HOLineSchCovItem_alfa'',''Dwelling_HOE'',''HOLineSchExclItem_alfa'') then ''INSRNC_LOB_TYPE8''

when pc_etlclausepattern.OwningEntityType_stg in (''BP7BldgSchedCovItem'', ''BP7Building'', ''BP7BusinessOwnersLine'', ''BP7Classification'', ''BP7LineSchedCovItem'', ''BP7Location'', ''BP7LocSchedCovItem'') then ''INSRNC_LOB_TYPE10''

when pc_etlclausepattern.OwningEntityType_stg  in (''PUPPersonalUmbrellaLine'',''PUPPersonalUmbrellaLineScheduleExclItem'') then ''INSRNC_LOB_TYPE11''

when pc_etlclausepattern.OwningEntityType_stg  in (''FOPBlanket'',''FOPDwelling'',''FOPDwellingScheduleCovItem'',''FOPDwellingScheduleExclItem'',

''FOPFarmownersLineScheduleCovItem'',''FOPFeedAndSeed'',''FOPLiability'',''FOPLiabilityScheduleCovItem'',''FOPLiabilityScheduleExclItem'',''FOPLivestock'',

''FOPMachinery'',''FOPOutbuilding'',''FOPFarmownersLine'')then ''INSRNC_LOB_TYPE_12''

else '' ''

end INSRNC_LOB_TYPE_CD ,

pc_etlclausepattern.OwningEntityType_stg as FEAT_COVERABLE_TYPE_TXT,

pc_etlcovtermpattern.patternid_stg as NK_SRC_KEY,

pc_etlcovtermpattern.covtermtype_stg as FEAT_DATA_TYPE_NAME,

pc_etlcovtermpattern.ModelType_stg as FEAT_DTL_MODL_TYPE_NAME,

'' '' as FEAT_DTL_CD_NAME,

'' '' as FEAT_DTL_VAL,

pc_etlcovtermpattern.ValueType_stg as FEAT_DTL_VAL_TYPE,

pc_etlcovtermpattern.columnname_stg as FEAT_DTL_COL_NAME,

cast (pc_etlclausepattern.name_stg  as varchar (255)) as INSRNC_CVGE_TYPE_CD

FROM DB_T_PROD_STAG.pc_etlcovtermpattern pc_etlcovtermpattern 

 inner JOIN DB_T_PROD_STAG.pc_etlclausepattern pc_etlclausepattern on pc_etlclausepattern.id_stg=pc_etlcovtermpattern.ClausePatternID_stg  

/* where  OwningEntityType in (''PersonalAutoLine'',''PersonalVehicle'',''PAWatercraftTrailer_alfa'',''PAWatercraftMotor_alfa'',''HomeownersLine_HOE'',''HOLineSchCovItem_alfa'',''Dwelling_HOE'',''HOLineSchExclItem_alfa'') */


union



select



''FEAT_SBTYPE9'' as FEAT_SBTYPE_CD, /* Package */
pc_etlclausepattern.clausetype_stg as FEAT_INSRNC_SBTYPE_CD,

'' '' as FEAT_CLASFCN_CD,

(pc_etlcovtermpattern.modeltype_stg || ''-''  || pc_etlcovtermpattern.name_stg  || ''-''  || pc_etlcovtermpackage.packagecode_stg || ''-''  || pc_etlcovtermpackage.name_stg) as FEAT_DESC,

( pc_etlcovtermpackage.packagecode_stg || ''-''  || pc_etlcovtermpackage.name_stg) as FEAT_NAME,

(pc_etlcovtermpattern.modeltype_stg || ''-''  || pc_etlcovtermpattern.name_stg  || ''-''  || pc_etlcovtermpackage.packagecode_stg || ''-''  || pc_etlcovtermpackage.name_stg) as COMN_FEAT_NAME,

''2'' as FEAT_LVL_SBTYPE_CNT,

case

when pc_etlclausepattern.OwningEntityType_stg in (''PersonalAutoLine'',''PersonalVehicle'',''PAWatercraftTrailer_alfa'',''PAWatercraftMotor_alfa'') then ''INSRNC_LOB_TYPE9''

when  pc_etlclausepattern.OwningEntityType_stg in (''HomeownersLine_HOE'',''HOLineSchCovItem_alfa'',''Dwelling_HOE'',''HOLineSchExclItem_alfa'') then ''INSRNC_LOB_TYPE8''

when pc_etlclausepattern.OwningEntityType_stg in (''BP7BldgSchedCovItem'', ''BP7Building'', ''BP7BusinessOwnersLine'', ''BP7Classification'', ''BP7LineSchedCovItem'', ''BP7Location'', ''BP7LocSchedCovItem'') then ''INSRNC_LOB_TYPE10''

when pc_etlclausepattern.OwningEntityType_stg  in (''PUPPersonalUmbrellaLine'',''PUPPersonalUmbrellaLineScheduleExclItem'') then ''INSRNC_LOB_TYPE11''

when pc_etlclausepattern.OwningEntityType_stg  in (''FOPBlanket'',''FOPDwelling'',''FOPDwellingScheduleCovItem'',''FOPDwellingScheduleExclItem'',

''FOPFarmownersLineScheduleCovItem'',''FOPFeedAndSeed'',''FOPLiability'',''FOPLiabilityScheduleCovItem'',''FOPLiabilityScheduleExclItem'',''FOPLivestock'',

''FOPMachinery'',''FOPOutbuilding'',''FOPFarmownersLine'')then ''INSRNC_LOB_TYPE_12''

else '' ''

end INSRNC_LOB_TYPE_CD ,

pc_etlclausepattern.OwningEntityType_stg as FEAT_COVERABLE_TYPE_TXT,

pc_etlcovtermpackage.patternid_stg as NK_SRC_KEY,

pc_etlcovtermpattern.covtermtype_stg as FEAT_DATA_TYPE_NAME,

pc_etlcovtermpattern.ModelType_stg as FEAT_DTL_MODL_TYPE_NAME,

pc_etlcovtermpackage.packagecode_stg as FEAT_DTL_CD_NAME,

'' ''   as FEAT_DTL_VAL,

pc_etlcovtermpattern.ValueType_stg as FEAT_DTL_VAL_TYPE,

pc_etlcovtermpattern.columnname_stg as FEAT_DTL_COL_NAME,

cast (pc_etlclausepattern.name_stg  as varchar (255)) as INSRNC_CVGE_TYPE_CD

FROM DB_T_PROD_STAG.pc_etlcovtermpattern pc_etlcovtermpattern 

inner JOIN DB_T_PROD_STAG.pc_etlclausepattern pc_etlclausepattern on pc_etlclausepattern.id_stg=pc_etlcovtermpattern.ClausePatternID_stg

inner JOIN DB_T_PROD_STAG.pc_etlcovtermpackage pc_etlcovtermpackage on pc_etlcovtermpackage.CoverageTermPatternID_stg=pc_etlcovtermpattern.id_stg

/* where  OwningEntityType in (''PersonalAutoLine'',''PersonalVehicle'',''PAWatercraftTrailer_alfa'',''PAWatercraftMotor_alfa'',''HomeownersLine_HOE'',''HOLineSchCovItem_alfa'',''Dwelling_HOE'',''HOLineSchExclItem_alfa'') */


union





select



''FEAT_SBTYPE10'' as FEAT_SBTYPE_CD, /* Package Term */
pc_etlclausepattern.clausetype_stg as FEAT_INSRNC_SBTYPE_CD,

'' '' as FEAT_CLASFCN_CD,

(pc_etlcovtermpattern.modeltype_stg || ''-''  || pc_etlcovtermpattern.name_stg|| ''-''  || pc_etlcovtermpackage.packagecode_stg || ''-''  || pc_etlcovtermpackage.name_stg  || ''-''  || pc_etlpackterm.name_stg) as FEAT_DESC,

(pc_etlcovtermpattern.modeltype_stg || ''-''  || pc_etlcovtermpattern.name_stg  || ''-''  || pc_etlcovtermpackage.packagecode_stg || ''-''  || pc_etlcovtermpackage.name_stg  || ''-''  || pc_etlpackterm.name_stg) as FEAT_NAME,

(pc_etlcovtermpattern.modeltype_stg || ''-''  || pc_etlcovtermpattern.name_stg  || ''-''  || pc_etlcovtermpackage.packagecode_stg || ''-''  || pc_etlcovtermpackage.name_stg || ''-''  || pc_etlpackterm.name_stg) as COMN_FEAT_NAME,

''3'' as FEAT_LVL_SBTYPE_CNT,

case

when pc_etlclausepattern.OwningEntityType_stg in (''PersonalAutoLine'',''PersonalVehicle'',''PAWatercraftTrailer_alfa'',''PAWatercraftMotor_alfa'') then ''INSRNC_LOB_TYPE9''

when  pc_etlclausepattern.OwningEntityType_stg in (''HomeownersLine_HOE'',''HOLineSchCovItem_alfa'',''Dwelling_HOE'',''HOLineSchExclItem_alfa'') then ''INSRNC_LOB_TYPE8''

when pc_etlclausepattern.OwningEntityType_stg in (''BP7BldgSchedCovItem'', ''BP7Building'', ''BP7BusinessOwnersLine'', ''BP7Classification'', ''BP7LineSchedCovItem'', ''BP7Location'', ''BP7LocSchedCovItem'') then ''INSRNC_LOB_TYPE10''

when pc_etlclausepattern.OwningEntityType_stg  in (''PUPPersonalUmbrellaLine'',''PUPPersonalUmbrellaLineScheduleExclItem'') then ''INSRNC_LOB_TYPE11''

when pc_etlclausepattern.OwningEntityType_stg  in (''FOPBlanket'',''FOPDwelling'',''FOPDwellingScheduleCovItem'',''FOPDwellingScheduleExclItem'',

''FOPFarmownersLineScheduleCovItem'',''FOPFeedAndSeed'',''FOPLiability'',''FOPLiabilityScheduleCovItem'',''FOPLiabilityScheduleExclItem'',''FOPLivestock'',

''FOPMachinery'',''FOPOutbuilding'',''FOPFarmownersLine'')then ''INSRNC_LOB_TYPE_12''

else '' ''

end INSRNC_LOB_TYPE_CD ,

pc_etlclausepattern.OwningEntityType_stg as FEAT_COVERABLE_TYPE_TXT,

pc_etlpackterm.patternid_stg as NK_SRC_KEY,

pc_etlcovtermpattern.covtermtype_stg as FEAT_DATA_TYPE_NAME,

pc_etlcovtermpattern.ModelType_stg as FEAT_DTL_MODL_TYPE_NAME,

pc_etlpackterm.name_stg as FEAT_DTL_CD_NAME,

cast(pc_etlpackterm.value_stg as varchar (50)  ) as FEAT_DTL_VAL,

pc_etlpackterm.ValueType_stg   as FEAT_DTL_VAL_TYPE,

pc_etlcovtermpattern.columnname_stg as FEAT_DTL_COL_NAME,

cast (pc_etlclausepattern.name_stg  as varchar (255)) as INSRNC_CVGE_TYPE_CD



FROM DB_T_PROD_STAG.pc_etlcovtermpattern pc_etlcovtermpattern

inner JOIN DB_T_PROD_STAG.pc_etlclausepattern pc_etlclausepattern on pc_etlclausepattern.id_stg=pc_etlcovtermpattern.ClausePatternID_stg

inner JOIN DB_T_PROD_STAG.pc_etlcovtermpackage pc_etlcovtermpackage on pc_etlcovtermpackage.CoverageTermPatternID_stg=pc_etlcovtermpattern.id_stg

inner JOIN DB_T_PROD_STAG.pc_etlpackterm pc_etlpackterm on pc_etlpackterm.CovTermPackID_stg=pc_etlcovtermpackage.id_stg

/* where  OwningEntityType in (''PersonalAutoLine'',''PersonalVehicle'',''PAWatercraftTrailer_alfa'',''PAWatercraftMotor_alfa'',''HomeownersLine_HOE'',''HOLineSchCovItem_alfa'',''Dwelling_HOE'',''HOLineSchExclItem_alfa'') */


union



select



''FEAT_SBTYPE8'' as FEAT_SBTYPE_CD, /* Option */
pc_etlclausepattern.clausetype_stg as FEAT_INSRNC_SBTYPE_CD,

'' '' as FEAT_CLASFCN_CD,

(pc_etlcovtermpattern.name_stg || ''-''  || pc_etlcovtermpattern.modeltype_stg || ''-''  || pc_etlcovtermoption.optioncode_stg) as FEAT_DESC,

pc_etlcovtermoption.optioncode_stg as FEAT_NAME,

(pc_etlcovtermpattern.name_stg || ''-'' || pc_etlcovtermpattern.modeltype_stg  || ''-''  || pc_etlcovtermoption.optioncode_stg) as COMN_FEAT_NAME,

''2'' as FEAT_LVL_SBTYPE_CNT,

case

when pc_etlclausepattern.OwningEntityType_stg in (''PersonalAutoLine'',''PersonalVehicle'',''PAWatercraftTrailer_alfa'',''PAWatercraftMotor_alfa'') then ''INSRNC_LOB_TYPE9''

when  pc_etlclausepattern.OwningEntityType_stg in (''HomeownersLine_HOE'',''HOLineSchCovItem_alfa'',''Dwelling_HOE'',''HOLineSchExclItem_alfa'') then ''INSRNC_LOB_TYPE8''

when pc_etlclausepattern.OwningEntityType_stg in (''BP7BldgSchedCovItem'', ''BP7Building'', ''BP7BusinessOwnersLine'', ''BP7Classification'', ''BP7LineSchedCovItem'', ''BP7Location'', ''BP7LocSchedCovItem'') then ''INSRNC_LOB_TYPE10''

when pc_etlclausepattern.OwningEntityType_stg  in (''PUPPersonalUmbrellaLine'',''PUPPersonalUmbrellaLineScheduleExclItem'') then ''INSRNC_LOB_TYPE11''

when pc_etlclausepattern.OwningEntityType_stg  in (''FOPBlanket'',''FOPDwelling'',''FOPDwellingScheduleCovItem'',''FOPDwellingScheduleExclItem'',

''FOPFarmownersLineScheduleCovItem'',''FOPFeedAndSeed'',''FOPLiability'',''FOPLiabilityScheduleCovItem'',''FOPLiabilityScheduleExclItem'',''FOPLivestock'',

''FOPMachinery'',''FOPOutbuilding'',''FOPFarmownersLine'')then ''INSRNC_LOB_TYPE_12''

else '' ''

end INSRNC_LOB_TYPE_CD ,

pc_etlclausepattern.OwningEntityType_stg as FEAT_COVERABLE_TYPE_TXT,

pc_etlcovtermoption.patternid_stg as NK_SRC_KEY,

pc_etlcovtermpattern.covtermtype_stg as FEAT_DATA_TYPE_NAME,

pc_etlcovtermpattern.ModelType_stg as FEAT_DTL_MODL_TYPE_NAME,

pc_etlcovtermoption.optioncode_stg as FEAT_DTL_CD_NAME,

cast (pc_etlcovtermoption.value_stg as varchar (50))  as FEAT_DTL_VAL,

pc_etlcovtermpattern.ValueType_stg as FEAT_DTL_VAL_TYPE,

pc_etlcovtermpattern.columnname_stg as FEAT_DTL_COL_NAME,

cast (pc_etlclausepattern.name_stg  as varchar (255)) as INSRNC_CVGE_TYPE_CD

FROM DB_T_PROD_STAG.pc_etlcovtermpattern pc_etlcovtermpattern

inner JOIN DB_T_PROD_STAG.pc_etlclausepattern pc_etlclausepattern on pc_etlclausepattern.id_stg=pc_etlcovtermpattern.ClausePatternID_stg

inner JOIN DB_T_PROD_STAG.pc_etlcovtermoption pc_etlcovtermoption on pc_etlcovtermoption.CoverageTermPatternID_stg=pc_etlcovtermpattern.id_stg

/* where  OwningEntityType in (''PersonalAutoLine'',''PersonalVehicle'',''PAWatercraftTrailer_alfa'',''PAWatercraftMotor_alfa'',''HomeownersLine_HOE'',''HOLineSchCovItem_alfa'',''Dwelling_HOE'',''HOLineSchExclItem_alfa'') */


UNION



select 

''FEAT_SBTYPE13'' as FEAT_SBTYPE_CD,

''FEAT_INSRNC_SBTYPE3'' as FEAT_INSRNC_SBTYPE_CD ,

''FEAT_CLASFCN_TYPE5'' as FEAT_CLASFCN_CD,

  pctl_exclusiontype_alfa.DESCRIPTION_stg as description , 

  pctl_exclusiontype_alfa.name_stg as FEAT_NAME ,

  pctl_exclusiontype_alfa.name_stg as COMN_FEAT_NAME ,

''1'' as FEAT_LVL_SBTYPE_CNT , 

''INSRNC_LOB_TYPE9'' as INSRNC_LOB_TYPE_CD ,

'''' as FEAT_COVERABLE_TYPE_TXT ,

pctl_exclusiontype_alfa.typecode_stg as NK_SRC_KEY,

'''' as FEAT_DATA_TYPE_NAME ,

   '''' as FEAT_DTL_MODL_TYPE_NAME ,

  '''' as FEAT_DTL_CD_NAME ,

  '''' as FEAT_DTL_VAL ,

  '''' as FEAT_DTL_VAL_TYPE , 

  '''' as FEAT_DTL_COL_NAME ,

  '''' as INSRNC_CVGE_TYPE_CD 

  

from DB_T_PROD_STAG.pctl_exclusiontype_alfa



UNION 



select 

''FEAT_SBTYPE14'' as FEAT_SBTYPE_CD,

''FEAT_INSRNC_SBTYPE4'' as FEAT_INSRNC_SBTYPE_CD ,

''FEAT_CLASFCN_TYPE5'' as FEAT_CLASFCN_CD,

  pctl_waivedreason_alfa.DESCRIPTION_stg as description , 

  pctl_waivedreason_alfa.name_stg as FEAT_NAME ,

  pctl_waivedreason_alfa.name_stg as COMN_FEAT_NAME ,

''1'' as FEAT_LVL_SBTYPE_CNT , 

''INSRNC_LOB_TYPE9'' as INSRNC_LOB_TYPE_CD ,

'''' as FEAT_COVERABLE_TYPE_TXT ,

pctl_waivedreason_alfa.typecode_stg  as NK_SRC_KEY,

'''' as FEAT_DATA_TYPE_NAME ,

   '''' as FEAT_DTL_MODL_TYPE_NAME ,

  '''' as FEAT_DTL_CD_NAME ,

  '''' as FEAT_DTL_VAL ,

  '''' as FEAT_DTL_VAL_TYPE , 

  '''' as FEAT_DTL_COL_NAME ,

  '''' as INSRNC_CVGE_TYPE_CD 

  

from DB_T_PROD_STAG.pctl_waivedreason_alfa



UNION





select 

''FEAT_SBTYPE15'' as FEAT_SBTYPE_CD,

pctl_documenttype.typecode_stg as FEAT_INSRNC_SBTYPE_CD,

'''' as FEAT_CLASFCN_CD,

pc_formpattern.description_stg as description,

pc_formpattern.formnumber_stg  as FEAT_NAME,

pc_formpattern.refcode_stg as COMN_FEAT_NAME ,

''1'' as  FEAT_LVL_SBTYPE_CNT , 

case when pc_formpattern.PolicyLinePatternCode_stg in (''PersonalAutoLine'',''PersonalVehicle'',''PAWatercraftTrailer_alfa'',''PAWatercraftMotor_alfa'')  

then ''INSRNC_LOB_TYPE9'' 

when pc_formpattern.PolicyLinePatternCode_stg in (''HomeownersLine_HOE'',''HOLineSchCovItem_alfa'',''Dwelling_HOE'',''HOLineSchExclItem_alfa'') 

then ''INSRNC_LOB_TYPE8'' 

when pc_formpattern.PolicyLinePatternCode_stg in (''BP7Line'') then ''INSRNC_LOB_TYPE10''

when pc_formpattern.PolicyLinePatternCode_stg in (''PUPPersonalUmbrellaLine'',''PUPPersonalUmbrellaLineScheduleExclItem'') then ''INSRNC_LOB_TYPE11''

when pc_formpattern.PolicyLinePatternCode_stg in (''FarmownersLine'') then ''INSRNC_LOB_TYPE_12''

else '' '' end as INSRNC_LOB_TYPE_CD ,

pc_formpattern.policylinepatterncode_stg as FEAT_COVERABLE_TYPE_TXT ,

pc_formpattern.code_stg as NK_SRC_KEY,

'''' as FEAT_DATA_TYPE_NAME ,

   '''' as FEAT_DTL_MODL_TYPE_NAME ,

  '''' as FEAT_DTL_CD_NAME ,

  '''' as FEAT_DTL_VAL ,

  '''' as FEAT_DTL_VAL_TYPE , 

  '''' as FEAT_DTL_COL_NAME ,

  '''' as INSRNC_CVGE_TYPE_CD 

from DB_T_PROD_STAG.pc_formpattern 

inner join DB_T_PROD_STAG.pctl_documenttype  on pctl_documenttype.id_stg = pc_formpattern.DocumentType_stg

where pctl_documenttype.typecode_stg = ''endorsement_alfa''

/* and ClausePatternCode is not null */
and pc_formpattern.Retired_stg = 0



) as TMP

order by FEAT_SBTYPE_CD,FEAT_CLASFCN_CD,FEAT_DESC asc nulls last
) SRC
)
);


-- Component exp_pass_to_src, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_src AS
(
SELECT
LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_FEAT_SBTYPE_CD */ as OUT_FEAT_SBTYPE_CD,
LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_FEAT_INSRNC_SBTYPE_CD */ as out_FEAT_INSRNC_SBTYPE_CD,
LTRIM ( RTRIM ( SQ_feat.FEAT_CLASFCN_CD ) ) as var_FEAT_CLASFCN_CD,
CASE WHEN TRIM(var_FEAT_CLASFCN_CD) = '''' OR var_FEAT_CLASFCN_CD IS NULL OR LENGTH ( var_FEAT_CLASFCN_CD ) = 0 THEN ''UNK'' ELSE LKP_3.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_FEAT_CLASFCN_CD */ END as out_FEAT_CLASFCN_CD,
CASE WHEN SQ_feat.FEAT_DESC IS NULL THEN '' '' ELSE SQ_feat.FEAT_DESC END as FEAT_DESC1,
CASE WHEN SQ_feat.FEAT_NAME IS NULL THEN '' '' ELSE SQ_feat.FEAT_NAME END as FEAT_NAME1,
CASE WHEN SQ_feat.COMN_FEAT_NAME IS NULL THEN '' '' ELSE SQ_feat.COMN_FEAT_NAME END as COMN_FEAT_NAME1,
CASE WHEN SQ_feat.FEAT_LVL_SBTYPE_CNT IS NULL THEN - 1 ELSE SQ_feat.FEAT_LVL_SBTYPE_CNT END as FEAT_LVL_SBTYPE_CNT1,
LTRIM ( RTRIM ( SQ_feat.INSRNC_LOB_TYPE_CD ) ) as var_INSRNC_LOB_TYPE_CD,
CASE WHEN TRIM(var_INSRNC_LOB_TYPE_CD) = '''' OR var_INSRNC_LOB_TYPE_CD IS NULL OR LENGTH ( var_INSRNC_LOB_TYPE_CD ) = 0 THEN ''UNK'' ELSE LKP_4.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_INSRNC_LOB_TYPE_CD */ END as var_INSRNC_LOB_TYPE_CD2,
CASE WHEN TRIM(var_INSRNC_LOB_TYPE_CD2) = '''' OR var_INSRNC_LOB_TYPE_CD2 IS NULL OR LENGTH ( var_INSRNC_LOB_TYPE_CD2 ) = 0 THEN ''UNK'' ELSE var_INSRNC_LOB_TYPE_CD2 END as out_INSRNC_LOB_TYPE_CD,
SQ_feat.FEAT_COVERABLE_TYPE_TXT as FEAT_COVERABLE_TYPE_TXT,
CASE WHEN SQ_feat.FEAT_COVERABLE_TYPE_TXT IS NULL THEN '' '' ELSE SQ_feat.FEAT_COVERABLE_TYPE_TXT END as FEAT_COVERABLE_TYPE_TXT1,
SQ_feat.NK_SRC_KEY as NK_SRC_KEY,
CASE WHEN SQ_feat.FEAT_DATA_TYPE_NAME IS NULL THEN '' '' ELSE SQ_feat.FEAT_DATA_TYPE_NAME END as FEAT_DATA_TYPE_NAME1,
SQ_feat.FEAT_DTL_MODL_TYPE_NAME as FEAT_DTL_MODL_TYPE_NAME,
CASE WHEN SQ_feat.FEAT_DTL_CD_NAME IS NULL THEN '' '' ELSE SQ_feat.FEAT_DTL_CD_NAME END as FEAT_DTL_CD_NAME1,
SQ_feat.FEAT_DTL_VAL as FEAT_DTL_VAL,
SQ_feat.FEAT_DTL_VAL_TYPE as FEAT_DTL_VAL_TYPE,
SQ_feat.FEAT_DTL_COL_NAME as FEAT_DTL_COL_NAME,
LKP_5.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_FEAT_COVRAGE_CD */ as OUT_INSRNC_CVGE_TYPE_CD,
to_date ( ''01/01/1900'' , ''mm/dd/yyyy'' ) as default_date,
CURRENT_TIMESTAMP as EDW_STRT_DTTM,
TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as EDW_END_DTTM,
dateadd (second, -1, CURRENT_TIMESTAMP ) as EDW_expiry,
SQ_feat.source_record_id,
row_number() over (partition by SQ_feat.source_record_id order by SQ_feat.source_record_id) as RNK
FROM
SQ_feat
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_FEAT_SBTYPE_CD LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = SQ_feat.FEAT_SBTYPE_CD
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_FEAT_INSRNC_SBTYPE_CD LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = SQ_feat.FEAT_INSRNC_SBTYPE_CD
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_FEAT_CLASFCN_CD LKP_3 ON LKP_3.SRC_IDNTFTN_VAL = SQ_feat.FEAT_CLASFCN_CD
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_INSRNC_LOB_TYPE_CD LKP_4 ON LKP_4.SRC_IDNTFTN_VAL = SQ_feat.INSRNC_LOB_TYPE_CD
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_FEAT_COVRAGE_CD LKP_5 ON LKP_5.SRC_IDNTFTN_VAL = SQ_feat.INSRNC_CVGE_TYPE_CD
QUALIFY RNK = 1
);


-- Component LKP_FEAT, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_FEAT AS
(
SELECT
LKP.FEAT_ID,
LKP.FEAT_SBTYPE_CD,
LKP.FEAT_INSRNC_SBTYPE_CD,
LKP.FEAT_CLASFCN_CD,
LKP.FEAT_DESC,
LKP.FEAT_NAME,
LKP.COMN_FEAT_NAME,
LKP.FEAT_LVL_SBTYPE_CNT,
LKP.INSRNC_CVGE_TYPE_CD,
LKP.INSRNC_LOB_TYPE_CD,
LKP.FEAT_DATA_TYPE_NAME,
LKP.FEAT_COVERABLE_TYPE_TXT,
LKP.FEAT_DTL_MODL_TYPE_NAME,
LKP.FEAT_DTL_CD_NAME,
LKP.FEAT_DTL_VAL,
LKP.FEAT_DTL_VAL_TYPE,
LKP.FEAT_DTL_COL_NAME,
LKP.NK_SRC_KEY,
LKP.EDW_STRT_DTTM,
LKP.EDW_END_DTTM,
exp_pass_to_src.FEAT_DTL_CD_NAME1 as FEAT_DTL_CD_NAME1,
row_number() over (order by 1) as NEXTVAL,
exp_pass_to_src.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_to_src.source_record_id ORDER BY LKP.FEAT_ID asc,LKP.FEAT_SBTYPE_CD asc,LKP.FEAT_INSRNC_SBTYPE_CD asc,LKP.FEAT_CLASFCN_CD asc,LKP.FEAT_DESC asc,LKP.FEAT_NAME asc,LKP.COMN_FEAT_NAME asc,LKP.FEAT_LVL_SBTYPE_CNT asc,LKP.INSRNC_CVGE_TYPE_CD asc,LKP.INSRNC_LOB_TYPE_CD asc,LKP.FEAT_DATA_TYPE_NAME asc,LKP.FEAT_COVERABLE_TYPE_TXT asc,LKP.FEAT_DTL_MODL_TYPE_NAME asc,LKP.FEAT_DTL_CD_NAME asc,LKP.FEAT_DTL_VAL asc,LKP.FEAT_DTL_VAL_TYPE asc,LKP.FEAT_DTL_COL_NAME asc,LKP.NK_SRC_KEY asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc) RNK
FROM
exp_pass_to_src
LEFT JOIN (
SELECT FEAT.FEAT_ID as FEAT_ID, FEAT.FEAT_INSRNC_SBTYPE_CD as FEAT_INSRNC_SBTYPE_CD, FEAT.FEAT_CLASFCN_CD as FEAT_CLASFCN_CD, FEAT.FEAT_DESC as FEAT_DESC, FEAT.FEAT_NAME as FEAT_NAME, FEAT.COMN_FEAT_NAME as COMN_FEAT_NAME, FEAT.FEAT_LVL_SBTYPE_CNT as FEAT_LVL_SBTYPE_CNT, FEAT.INSRNC_CVGE_TYPE_CD as INSRNC_CVGE_TYPE_CD, FEAT.INSRNC_LOB_TYPE_CD as INSRNC_LOB_TYPE_CD, FEAT.FEAT_DATA_TYPE_NAME as FEAT_DATA_TYPE_NAME, FEAT.FEAT_COVERABLE_TYPE_TXT as FEAT_COVERABLE_TYPE_TXT,  FEAT.FEAT_DTL_MODL_TYPE_NAME as FEAT_DTL_MODL_TYPE_NAME, FEAT.FEAT_DTL_CD_NAME as FEAT_DTL_CD_NAME, FEAT.FEAT_DTL_VAL as FEAT_DTL_VAL, FEAT.FEAT_DTL_VAL_TYPE as FEAT_DTL_VAL_TYPE, FEAT.FEAT_DTL_COL_NAME as FEAT_DTL_COL_NAME, FEAT.EDW_STRT_DTTM as EDW_STRT_DTTM, FEAT.EDW_END_DTTM as EDW_END_DTTM, FEAT.FEAT_SBTYPE_CD as FEAT_SBTYPE_CD, FEAT.NK_SRC_KEY as NK_SRC_KEY FROM DB_T_PROD_CORE.FEAT
QUALIFY ROW_NUMBER() OVER(PARTITION BY FEAT_COVERABLE_TYPE_TXT, NK_SRC_KEY, FEAT_SBTYPE_CD 
ORDER BY EDW_END_DTTM desc) = 1
/* where EDW_END_DTTM=TO_DATE(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'') */
) LKP ON LKP.FEAT_SBTYPE_CD = exp_pass_to_src.OUT_FEAT_SBTYPE_CD AND LKP.NK_SRC_KEY = exp_pass_to_src.NK_SRC_KEY AND LKP.FEAT_COVERABLE_TYPE_TXT = exp_pass_to_src.FEAT_COVERABLE_TYPE_TXT1
QUALIFY RNK = 1
);


-- Component exp_feat_lkp, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_feat_lkp AS
(
SELECT
LKP_FEAT.FEAT_ID as lkp_FEAT_ID,
LKP_FEAT.NEXTVAL as FEAT_ID,
exp_pass_to_src.OUT_FEAT_SBTYPE_CD as FEAT_SBTYPE_CD,
exp_pass_to_src.out_FEAT_INSRNC_SBTYPE_CD as FEAT_INSRNC_SBTYPE_CD,
exp_pass_to_src.out_FEAT_CLASFCN_CD as FEAT_CLASFCN_CD,
exp_pass_to_src.FEAT_DESC1 as FEAT_DESC,
exp_pass_to_src.FEAT_NAME1 as FEAT_NAME,
exp_pass_to_src.COMN_FEAT_NAME1 as COMN_FEAT_NAME,
exp_pass_to_src.FEAT_LVL_SBTYPE_CNT1 as FEAT_LVL_SBTYPE_CNT,
exp_pass_to_src.out_INSRNC_LOB_TYPE_CD as INSRNC_LOB_TYPE_CD,
exp_pass_to_src.FEAT_DATA_TYPE_NAME1 as FEAT_DATA_TYPE_NAME,
exp_pass_to_src.FEAT_COVERABLE_TYPE_TXT as FEAT_COVERABLE_TYPE_TXT,
:PRCS_ID as out_PRCS_ID,
exp_pass_to_src.FEAT_DTL_MODL_TYPE_NAME as FEAT_DTL_MODL_TYPE_NAME,
LKP_FEAT.FEAT_DTL_CD_NAME1 as FEAT_DTL_CD_NAME,
exp_pass_to_src.FEAT_DTL_VAL as FEAT_DTL_VAL,
exp_pass_to_src.FEAT_DTL_VAL_TYPE as FEAT_DTL_VAL_TYPE,
exp_pass_to_src.FEAT_DTL_COL_NAME as FEAT_DTL_COL_NAME,
exp_pass_to_src.NK_SRC_KEY as NK_SRC_KEY,
exp_pass_to_src.OUT_INSRNC_CVGE_TYPE_CD as INSRNC_CVGE_TYPE_CD,
NULL as VAL_TYPE_CD,
--lit('''') as IS_MODIFIED,
LKP_FEAT.FEAT_SBTYPE_CD as lkp_FEAT_SBTYPE_CD1,
LKP_FEAT.FEAT_DESC as lkp_FEAT_DESC1,
LKP_FEAT.FEAT_NAME as lkp_FEAT_NAME1,
LKP_FEAT.COMN_FEAT_NAME as lkp_COMN_FEAT_NAME1,
LKP_FEAT.FEAT_LVL_SBTYPE_CNT as lkp_FEAT_LVL_SBTYPE_CNT1,
LKP_FEAT.INSRNC_LOB_TYPE_CD as lkp_INSRNC_LOB_TYPE_CD1,
LKP_FEAT.FEAT_DATA_TYPE_NAME as lkp_FEAT_DATA_TYPE_NAME1,
LKP_FEAT.FEAT_DTL_MODL_TYPE_NAME as lkp_FEAT_DTL_MODL_TYPE_NAME1,
LKP_FEAT.FEAT_DTL_CD_NAME as lkp_FEAT_DTL_CD_NAME1,
LKP_FEAT.NK_SRC_KEY as lkp_NK_SRC_KEY1,
LKP_FEAT.EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
md5 ( ltrim ( rtrim ( LKP_FEAT.FEAT_INSRNC_SBTYPE_CD ) ) || ltrim ( rtrim ( LKP_FEAT.FEAT_CLASFCN_CD ) ) || ltrim ( rtrim ( upper ( LKP_FEAT.FEAT_DESC ) ) ) || ltrim ( rtrim ( upper ( LKP_FEAT.FEAT_NAME ) ) ) || ltrim ( rtrim ( upper ( LKP_FEAT.COMN_FEAT_NAME ) ) ) || ltrim ( rtrim ( LKP_FEAT.FEAT_LVL_SBTYPE_CNT ) ) || ltrim ( rtrim ( upper ( LKP_FEAT.INSRNC_CVGE_TYPE_CD ) ) ) || ltrim ( rtrim ( upper ( LKP_FEAT.INSRNC_LOB_TYPE_CD ) ) ) || ltrim ( rtrim ( LKP_FEAT.FEAT_DATA_TYPE_NAME ) ) || ltrim ( rtrim ( LKP_FEAT.FEAT_COVERABLE_TYPE_TXT ) ) || ltrim ( rtrim ( LKP_FEAT.FEAT_DTL_MODL_TYPE_NAME ) ) || ltrim ( rtrim ( LKP_FEAT.FEAT_DTL_CD_NAME ) ) || ltrim ( rtrim ( LKP_FEAT.FEAT_DTL_VAL ) ) || ltrim ( rtrim ( LKP_FEAT.FEAT_DTL_VAL_TYPE ) ) || ltrim ( rtrim ( LKP_FEAT.FEAT_DTL_COL_NAME ) ) ) as chksum_lkp,
md5 ( ltrim ( rtrim ( exp_pass_to_src.out_FEAT_INSRNC_SBTYPE_CD ) ) || ltrim ( rtrim ( exp_pass_to_src.out_FEAT_CLASFCN_CD ) ) || ltrim ( rtrim ( upper ( exp_pass_to_src.FEAT_DESC1 ) ) ) || ltrim ( rtrim ( upper ( exp_pass_to_src.FEAT_NAME1 ) ) ) || ltrim ( rtrim ( upper ( exp_pass_to_src.COMN_FEAT_NAME1 ) ) ) || ltrim ( rtrim ( exp_pass_to_src.FEAT_LVL_SBTYPE_CNT1 ) ) || ltrim ( rtrim ( upper ( exp_pass_to_src.OUT_INSRNC_CVGE_TYPE_CD ) ) ) || ltrim ( rtrim ( upper ( exp_pass_to_src.out_INSRNC_LOB_TYPE_CD ) ) ) || ltrim ( rtrim ( exp_pass_to_src.FEAT_DATA_TYPE_NAME1 ) ) || ltrim ( rtrim ( exp_pass_to_src.FEAT_COVERABLE_TYPE_TXT ) ) || ltrim ( rtrim ( exp_pass_to_src.FEAT_DTL_MODL_TYPE_NAME ) ) || ltrim ( rtrim ( LKP_FEAT.FEAT_DTL_CD_NAME1 ) ) || ltrim ( rtrim ( exp_pass_to_src.FEAT_DTL_VAL ) ) || ltrim ( rtrim ( exp_pass_to_src.FEAT_DTL_VAL_TYPE ) ) || ltrim ( rtrim ( exp_pass_to_src.FEAT_DTL_COL_NAME ) ) ) as chksum_inp,
CASE WHEN chksum_lkp IS NULL THEN ''I'' ELSE CASE WHEN chksum_lkp != chksum_inp THEN ''U'' ELSE ''R'' END END as flag,
exp_pass_to_src.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_pass_to_src.EDW_END_DTTM as EDW_END_DTTM,
exp_pass_to_src.EDW_expiry as EDW_expiry,
exp_pass_to_src.default_date as default_date,
exp_pass_to_src.source_record_id
FROM
exp_pass_to_src
INNER JOIN LKP_FEAT ON exp_pass_to_src.source_record_id = LKP_FEAT.source_record_id
);


-- Component rtr_insert_update_update, Type ROUTER Output Group update
CREATE OR REPLACE TEMPORARY TABLE rtr_insert_update_update AS (
SELECT
exp_feat_lkp.FEAT_ID as FEAT_ID,
exp_feat_lkp.FEAT_SBTYPE_CD as FEAT_SBTYPE_CD,
exp_feat_lkp.FEAT_INSRNC_SBTYPE_CD as FEAT_INSRNC_SBTYPE_CD,
exp_feat_lkp.FEAT_CLASFCN_CD as FEAT_CLASFCN_CD,
exp_feat_lkp.FEAT_DESC as FEAT_DESC,
exp_feat_lkp.FEAT_NAME as FEAT_NAME,
exp_feat_lkp.COMN_FEAT_NAME as COMN_FEAT_NAME,
exp_feat_lkp.FEAT_LVL_SBTYPE_CNT as FEAT_LVL_SBTYPE_CNT,
exp_feat_lkp.INSRNC_CVGE_TYPE_CD as INSRNC_CVGE_TYPE_CD,
exp_feat_lkp.INSRNC_LOB_TYPE_CD as INSRNC_LOB_TYPE_CD,
exp_feat_lkp.FEAT_DATA_TYPE_NAME as FEAT_DATA_TYPE_NAME,
exp_feat_lkp.FEAT_COVERABLE_TYPE_TXT as FEAT_COVERABLE_TYPE_TXT,
exp_feat_lkp.out_PRCS_ID as PRCS_ID,
exp_feat_lkp.FEAT_DTL_MODL_TYPE_NAME as FEAT_DTL_MODL_TYPE_NAME,
exp_feat_lkp.FEAT_DTL_CD_NAME as FEAT_DTL_CD_NAME,
exp_feat_lkp.FEAT_DTL_VAL as FEAT_DTL_VAL,
exp_feat_lkp.FEAT_DTL_VAL_TYPE as FEAT_DTL_VAL_TYPE,
exp_feat_lkp.FEAT_DTL_COL_NAME as FEAT_DTL_COL_NAME,
--exp_feat_lkp.IS_MODIFIED as IS_MODIFIED,
exp_feat_lkp.NK_SRC_KEY as NK_SRC_KEY,
exp_feat_lkp.VAL_TYPE_CD as VAL_TYPE_CD,
exp_feat_lkp.lkp_FEAT_SBTYPE_CD1 as lkp_FEAT_SBTYPE_CD1,
exp_feat_lkp.lkp_NK_SRC_KEY1 as lkp_NK_SRC_KEY1,
exp_feat_lkp.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_feat_lkp.flag as flag,
exp_feat_lkp.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_feat_lkp.EDW_END_DTTM as EDW_END_DTTM,
exp_feat_lkp.EDW_expiry as EDW_expiry,
exp_feat_lkp.default_date as default_date,
exp_feat_lkp.lkp_FEAT_DESC1 as lkp_FEAT_DESC1,
exp_feat_lkp.lkp_FEAT_NAME1 as lkp_FEAT_NAME1,
exp_feat_lkp.lkp_COMN_FEAT_NAME1 as lkp_COMN_FEAT_NAME1,
exp_feat_lkp.lkp_FEAT_LVL_SBTYPE_CNT1 as lkp_FEAT_LVL_SBTYPE_CNT1,
exp_feat_lkp.lkp_INSRNC_LOB_TYPE_CD1 as lkp_INSRNC_LOB_TYPE_CD1,
exp_feat_lkp.lkp_FEAT_DATA_TYPE_NAME1 as lkp_FEAT_DATA_TYPE_NAME1,
exp_feat_lkp.lkp_FEAT_DTL_MODL_TYPE_NAME1 as lkp_FEAT_DTL_MODL_TYPE_NAME1,
exp_feat_lkp.lkp_FEAT_DTL_CD_NAME1 as lkp_FEAT_DTL_CD_NAME1,
exp_feat_lkp.lkp_FEAT_ID as lkp_FEAT_ID,
exp_feat_lkp.source_record_id
FROM
exp_feat_lkp
WHERE exp_feat_lkp.flag = ''U'' or exp_feat_lkp.flag = ''I''
);


-- Component upd_feat_updins, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_feat_updins AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_insert_update_update.FEAT_ID as FEAT_ID,
rtr_insert_update_update.FEAT_SBTYPE_CD as FEAT_SBTYPE_CD,
rtr_insert_update_update.FEAT_INSRNC_SBTYPE_CD as FEAT_INSRNC_SBTYPE_CD,
rtr_insert_update_update.FEAT_CLASFCN_CD as FEAT_CLASFCN_CD,
rtr_insert_update_update.FEAT_DESC as FEAT_DESC,
rtr_insert_update_update.FEAT_NAME as FEAT_NAME,
rtr_insert_update_update.COMN_FEAT_NAME as COMN_FEAT_NAME,
rtr_insert_update_update.FEAT_LVL_SBTYPE_CNT as FEAT_LVL_SBTYPE_CNT,
rtr_insert_update_update.INSRNC_CVGE_TYPE_CD as INSRNC_CVGE_TYPE_CD,
rtr_insert_update_update.INSRNC_LOB_TYPE_CD as INSRNC_LOB_TYPE_CD,
rtr_insert_update_update.FEAT_DATA_TYPE_NAME as FEAT_DATA_TYPE_NAME,
rtr_insert_update_update.FEAT_COVERABLE_TYPE_TXT as FEAT_COVERABLE_TYPE_TXT,
rtr_insert_update_update.PRCS_ID as PRCS_ID,
rtr_insert_update_update.FEAT_DTL_MODL_TYPE_NAME as FEAT_DTL_MODL_TYPE_NAME,
rtr_insert_update_update.FEAT_DTL_CD_NAME as FEAT_DTL_CD_NAME,
rtr_insert_update_update.FEAT_DTL_VAL as FEAT_DTL_VAL,
rtr_insert_update_update.FEAT_DTL_VAL_TYPE as FEAT_DTL_VAL_TYPE,
rtr_insert_update_update.FEAT_DTL_COL_NAME as FEAT_DTL_COL_NAME,
rtr_insert_update_update.NK_SRC_KEY as NK_SRC_KEY,
rtr_insert_update_update.VAL_TYPE_CD as VAL_TYPE_CD,
rtr_insert_update_update.EDW_STRT_DTTM as EDW_STRT_DTTM3,
rtr_insert_update_update.EDW_END_DTTM as EDW_END_DTTM3,
rtr_insert_update_update.default_date as default_date3,
rtr_insert_update_update.lkp_FEAT_ID as lkp_FEAT_ID3,
rtr_insert_update_update.flag as flag2,
rtr_insert_update_update.source_record_id,
0 as UPDATE_STRATEGY_ACTION
FROM
rtr_insert_update_update
);


-- Component exp_tgt_pass_insert1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_tgt_pass_insert1 AS
(
SELECT
CASE WHEN upd_feat_updins.flag2 = ''I'' THEN upd_feat_updins.FEAT_ID ELSE upd_feat_updins.lkp_FEAT_ID3 END as out_FEAT_ID,
upd_feat_updins.FEAT_SBTYPE_CD as FEAT_SBTYPE_CD,
upd_feat_updins.FEAT_INSRNC_SBTYPE_CD as FEAT_INSRNC_SBTYPE_CD,
upd_feat_updins.FEAT_CLASFCN_CD as FEAT_CLASFCN_CD,
upd_feat_updins.FEAT_DESC as FEAT_DESC,
upd_feat_updins.FEAT_NAME as FEAT_NAME,
upd_feat_updins.COMN_FEAT_NAME as COMN_FEAT_NAME,
upd_feat_updins.FEAT_LVL_SBTYPE_CNT as FEAT_LVL_SBTYPE_CNT,
upd_feat_updins.INSRNC_CVGE_TYPE_CD as INSRNC_CVGE_TYPE_CD,
upd_feat_updins.INSRNC_LOB_TYPE_CD as INSRNC_LOB_TYPE_CD,
upd_feat_updins.FEAT_DATA_TYPE_NAME as FEAT_DATA_TYPE_NAME,
upd_feat_updins.FEAT_COVERABLE_TYPE_TXT as FEAT_COVERABLE_TYPE_TXT,
:PRCS_ID as PRCS_ID,
upd_feat_updins.FEAT_DTL_MODL_TYPE_NAME as FEAT_DTL_MODL_TYPE_NAME,
upd_feat_updins.FEAT_DTL_CD_NAME as FEAT_DTL_CD_NAME,
upd_feat_updins.FEAT_DTL_VAL as FEAT_DTL_VAL,
upd_feat_updins.FEAT_DTL_VAL_TYPE as FEAT_DTL_VAL_TYPE,
upd_feat_updins.FEAT_DTL_COL_NAME as FEAT_DTL_COL_NAME,
upd_feat_updins.NK_SRC_KEY as NK_SRC_KEY,
upd_feat_updins.VAL_TYPE_CD as VAL_TYPE_CD,
upd_feat_updins.EDW_STRT_DTTM3 as EDW_STRT_DTTM3,
upd_feat_updins.EDW_END_DTTM3 as EDW_END_DTTM3,
upd_feat_updins.default_date3 as default_date3,
to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) as _Busn_end_dt,
upd_feat_updins.source_record_id
FROM
upd_feat_updins
);


-- Component tgt_feat_insert, Type TARGET 
INSERT INTO DB_T_PROD_CORE.FEAT
(
FEAT_ID,
FEAT_SBTYPE_CD,
FEAT_INSRNC_SBTYPE_CD,
FEAT_CLASFCN_CD,
FEAT_DESC,
FEAT_NAME,
COMN_FEAT_NAME,
FEAT_LVL_SBTYPE_CNT,
INSRNC_CVGE_TYPE_CD,
INSRNC_LOB_TYPE_CD,
FEAT_DATA_TYPE_NAME,
FEAT_COVERABLE_TYPE_TXT,
FEAT_DTL_MODL_TYPE_NAME,
FEAT_DTL_CD_NAME,
FEAT_DTL_VAL,
FEAT_DTL_VAL_TYPE,
FEAT_DTL_COL_NAME,
NK_SRC_KEY,
VAL_TYPE_CD,
PRCS_ID,
FEAT_STRT_DT,
FEAT_END_DT,
EDW_STRT_DTTM,
EDW_END_DTTM
)
SELECT
exp_tgt_pass_insert1.out_FEAT_ID as FEAT_ID,
exp_tgt_pass_insert1.FEAT_SBTYPE_CD as FEAT_SBTYPE_CD,
exp_tgt_pass_insert1.FEAT_INSRNC_SBTYPE_CD as FEAT_INSRNC_SBTYPE_CD,
exp_tgt_pass_insert1.FEAT_CLASFCN_CD as FEAT_CLASFCN_CD,
exp_tgt_pass_insert1.FEAT_DESC as FEAT_DESC,
left(exp_tgt_pass_insert1.FEAT_NAME,100) as FEAT_NAME,
left(exp_tgt_pass_insert1.COMN_FEAT_NAME,100) as COMN_FEAT_NAME,
exp_tgt_pass_insert1.FEAT_LVL_SBTYPE_CNT as FEAT_LVL_SBTYPE_CNT,
exp_tgt_pass_insert1.INSRNC_CVGE_TYPE_CD as INSRNC_CVGE_TYPE_CD,
exp_tgt_pass_insert1.INSRNC_LOB_TYPE_CD as INSRNC_LOB_TYPE_CD,
exp_tgt_pass_insert1.FEAT_DATA_TYPE_NAME as FEAT_DATA_TYPE_NAME,
exp_tgt_pass_insert1.FEAT_COVERABLE_TYPE_TXT as FEAT_COVERABLE_TYPE_TXT,
exp_tgt_pass_insert1.FEAT_DTL_MODL_TYPE_NAME as FEAT_DTL_MODL_TYPE_NAME,
exp_tgt_pass_insert1.FEAT_DTL_CD_NAME as FEAT_DTL_CD_NAME,
--exp_tgt_pass_insert1.FEAT_DTL_VAL 
  0 as FEAT_DTL_VAL,
exp_tgt_pass_insert1.FEAT_DTL_VAL_TYPE as FEAT_DTL_VAL_TYPE,
exp_tgt_pass_insert1.FEAT_DTL_COL_NAME as FEAT_DTL_COL_NAME,
exp_tgt_pass_insert1.NK_SRC_KEY as NK_SRC_KEY,
exp_tgt_pass_insert1.VAL_TYPE_CD as VAL_TYPE_CD,
exp_tgt_pass_insert1.PRCS_ID as PRCS_ID,
exp_tgt_pass_insert1.default_date3 as FEAT_STRT_DT,
exp_tgt_pass_insert1._Busn_end_dt as FEAT_END_DT,
exp_tgt_pass_insert1.EDW_STRT_DTTM3 as EDW_STRT_DTTM,
exp_tgt_pass_insert1.EDW_END_DTTM3 as EDW_END_DTTM
FROM
exp_tgt_pass_insert1;


-- Component tgt_feat_insert, Type Post SQL 
UPDATE  DB_T_PROD_CORE.FEAT 

set 

EDW_END_DTTM=A.lead
FROM

(SELECT	distinct FEAT_SBTYPE_CD, FEAT_COVERABLE_TYPE_TXT, NK_SRC_KEY, EDW_STRT_DTTM,

 

max(EDW_STRT_DTTM) over (partition by FEAT_SBTYPE_CD, FEAT_COVERABLE_TYPE_TXT, NK_SRC_KEY ORDER BY EDW_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND''

 as lead

FROM	DB_T_PROD_CORE.FEAT

 ) a



WHERE  FEAT.FEAT_SBTYPE_CD=A.FEAT_SBTYPE_CD

AND FEAT.FEAT_COVERABLE_TYPE_TXT=A.FEAT_COVERABLE_TYPE_TXT

AND FEAT.NK_SRC_KEY=A.NK_SRC_KEY

AND  FEAT.EDW_STRT_DTTM=A.EDW_STRT_DTTM

and lead is not null;


END; ';