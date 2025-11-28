-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_PROD_INSUPD("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
DECLARE 

start_dttm TIMESTAMP;
end_dttm TIMESTAMP;
PRCS_ID INTEGER;
P_DEFAULT_STR_CD char;
BEGIN 
start_dttm := CURRENT_TIMESTAMP();
end_dttm := CURRENT_TIMESTAMP();
PRCS_ID := 1;  

-- Component LKP_TERADATA_ETL_REF_XLAT, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

		FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM IN (''INSRNC_TYPE'' ,''PROD_SBTYPE'')

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS IN (''GW'',''DS'') 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM in (''pctl_bp7policytype_alfa.typecode'',''pctl_hopolicytype_hoe.typecode'', ''pctl_fopolicytype_alfa.typecode'',''pctl_papolicytype_alfa.typecode'',''derived'',''pctl_puppolicytype.typecode'')

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
		
		QUALIFY ROW_NUMBER() OVER (PARTITION BY SRC_IDNTFTN_VAL ORDER BY TGT_IDNTFTN_VAL DESC)=1 /*Added qualify to get the last value*/

		UNION

		SELECT 

	INSRNC_TYPE_CD

	,PROD_NAME 

		FROM 

	DB_T_PROD_STAG.PROD_STAG

WHERE 

	CAST(EDW_END_DTTM AS DATE)=''9999-12-31''--
);


-- Component LKP_TERADATA_ETL_REF_XLAT_INSRNC_LOB_TYPE, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_INSRNC_LOB_TYPE AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

		FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM = ''INSRNC_LOB_TYPE''

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS IN (''GW'',''DS'') 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM in (''pctl_bp7policytype_alfa.typecode'',''pctl_hopolicytype_hoe.typecode'', ''pctl_fopolicytype_alfa.typecode'',''pctl_papolicytype_alfa.typecode'',''gw_poltype_conv.gw_product_cd'', ''gw_poltype_conv.edm_inf_cnt_grp'',''pctl_puppolicytype.typecode'')

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
		
		QUALIFY ROW_NUMBER() OVER (PARTITION BY SRC_IDNTFTN_VAL ORDER BY TGT_IDNTFTN_VAL DESC)=1 /*Added qualify to get the last value*/

		UNION

		SELECT 

	INSRNC_LOB_TYPE_CD

	,PROD_NAME 

		FROM 

	DB_T_PROD_STAG.PROD_STAG

WHERE 

	CAST(EDW_END_DTTM AS DATE)=''9999-12-31''--
);


-- Component SQ_pcx_etlcovtermpattern, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_pcx_etlcovtermpattern AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as Code,
$2 as Name,
$3 as Description,
$4 as ID,
$5 as subtype,
$6 as STRT_DT,
$7 as END_DT,
$8 as Retired,
$9 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select LTRIM(RTRIM(typecode_stg)), name_stg, DESCRIPTION_stg,cast(id_stg as varchar(255)) as ID, cast(''POLICY TYPE''  as varchar(255)) as subtype,

TO_TIMESTAMP (:start_dttm) as STRT_DT, 

TO_TIMESTAMP (''9999-12-31 23:59:59.999999'') as END_DT

,Retired_stg as Retired

from  DB_T_PROD_STAG.pctl_papolicytype_alfa
union all
/* Hardcoding the Retired_stg value as 0 for EIM-32725 */
select LTRIM(RTRIM(typecode_stg)), name_stg, DESCRIPTION_stg,cast(id_stg as varchar(255)) as ID, cast(''POLICY TYPE'' as varchar(255)) as subtype,

TO_TIMESTAMP(:start_dttm) as STRT_DT, 

TO_TIMESTAMP(''9999-12-31 23:59:59.999999'') as END_DT

,0 as Retired

/* ,Retired_stg as Retired */
from DB_T_PROD_STAG.pctl_hopolicytype_hoe



union all



select LTRIM(RTRIM(UPPER(typecode_stg))), name_stg, DESCRIPTION_stg,cast(id_stg as varchar(255)) as ID, cast(''POLICY TYPE'' as varchar(255)) as subtype,

TO_TIMESTAMP(:start_dttm) as STRT_DT, 

TO_TIMESTAMP(''9999-12-31 23:59:59.999999'') as END_DT

,Retired_stg as Retired

from DB_T_PROD_STAG.pctl_bp7policytype_alfa



union all



select LTRIM(RTRIM(UPPER(typecode_stg))), name_stg, DESCRIPTION_stg,cast(id_stg as varchar(255)) as ID, cast(''BUSINESS TYPE'' as varchar(255)) as subtype,

to_TIMESTAMP(:start_dttm) as STRT_DT, 

TO_TIMESTAMP(''9999-12-31 23:59:59.999999'') as END_DT

,Retired_stg as Retired

from DB_T_PROD_STAG.pctl_bp7propertytype



Union



/******Product Domain from DB2 (Sprint 12)*******/



select distinct LTRIM(RTRIM(Cast(GW_PRODUCT_CD_stg as varchar(255)))) as typecode,Cast(GW_PRODUCT_CD_stg as varchar(255)) as name, Cast(GW_PRODUCT_CD_stg as varchar(255)) as description  ,''0'' as ID,''PRODUCT DOMAIN'' as sybtype, 

updatetime_stg as STRT_DT, 

TO_TIMESTAMP (''9999-12-31 23:59:59.999999'') as END_DT

,0 as Retired

from DB_T_PROD_STAG.GW_POLTYPE_CONV



union



select distinct LTRIM(RTRIM(Cast(EDM_INF_CNT_GRP_stg as varchar(255)))) as typecode,Cast(EDM_INF_CNT_GRP_stg as varchar(255)) as name, Cast(EDM_INF_CNT_GRP_stg as varchar(255)) as description  ,''0'' as ID,''PRODUCT DOMAIN'' as sybtype,

updatetime_stg as STRT_DT,

to_TIMESTAMP (''9999-12-31 23:59:59.999999'') as END_DT

,0 as Retired

from DB_T_PROD_STAG.GW_POLTYPE_CONV



union



/* ----Umbrella Changes-------- */
select LTRIM(RTRIM(typecode_stg)), name_stg, DESCRIPTION_stg,cast(id_stg as varchar(255)) as ID, cast(''POLICY TYPE''  as varchar(255)) as subtype,

to_TIMESTAMP(:start_dttm) as STRT_DT, 

to_TIMESTAMP (''9999-12-31 23:59:59.999999'') as END_DT

,Retired_stg as Retired

from DB_T_PROD_STAG.pctl_puppolicytype

UNION

/* ----FARM Changes-------- */
select LTRIM(RTRIM(typecode_stg)), name_stg, DESCRIPTION_stg,cast(id_stg as varchar(255)) as ID, cast(''POLICY TYPE''  as varchar(255)) as subtype,

to_TIMESTAMP (:start_dttm) as STRT_DT, 

to_TIMESTAMP (''9999-12-31 23:59:59.999999'') as END_DT

,Retired_stg as Retired

from DB_T_PROD_STAG.pctl_FOPpolicytype

UNION

/* ----FARM MAchinery & FCL Changes-------- */
select LTRIM(RTRIM(PROD_NAME)), PROD_NAME, PROD_DESC,cast(PROD_ID as varchar(255)) as ID, cast(''POLICY TYPE''  as varchar(255)) as subtype,

to_TIMESTAMP (:start_dttm) as STRT_DT, 

to_TIMESTAMP (''9999-12-31 23:59:59.999999'') as END_DT

,0 as Retired

from DB_T_PROD_STAG.prod_stag

where edw_end_dttm =  ''9999-12-31 23:59:59.999999''
) SRC
)
);


-- Component exp_pass_from_source, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_from_source AS
(
SELECT
SQ_pcx_etlcovtermpattern.Code as TypeCode,
SQ_pcx_etlcovtermpattern.Description as Description,
SQ_pcx_etlcovtermpattern.ID as ID,
rtrim ( ltrim ( SQ_pcx_etlcovtermpattern.Name ) ) as out_Name,
:PRCS_ID as PRCS_ID,
LTRIM ( RTRIM ( SQ_pcx_etlcovtermpattern.Code ) ) as var_INSRNC_LOB_TYPE_CD,
CASE WHEN TRIM(var_INSRNC_LOB_TYPE_CD) = '''' OR var_INSRNC_LOB_TYPE_CD IS NULL OR LENGTH ( var_INSRNC_LOB_TYPE_CD ) = 0 THEN ''UNK'' ELSE LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_INSRNC_LOB_TYPE */ END as var_INSRNC_LOB_TYPE_CD2,
CASE WHEN var_INSRNC_LOB_TYPE_CD2 IS NULL THEN ''UNK'' ELSE var_INSRNC_LOB_TYPE_CD2 END as out_INSRNC_LOB_TYPE_CD,
LTRIM ( RTRIM ( SQ_pcx_etlcovtermpattern.Code ) ) as var_INSRNC_TYPE_CD,
CASE WHEN TRIM(var_INSRNC_TYPE_CD) = '''' OR var_INSRNC_TYPE_CD IS NULL OR LENGTH ( var_INSRNC_TYPE_CD ) = 0 THEN ''UNK'' ELSE LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT */ END as var_INSRNC_TYPE_CD2,
CASE WHEN var_INSRNC_TYPE_CD2 IS NULL THEN ''UNK'' ELSE var_INSRNC_TYPE_CD2 END as out_INSRNC_TYPE_CD,
DECODE ( TRUE , SQ_pcx_etlcovtermpattern.subtype IS NOT NULL , LKP_3.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT */ , ''UNK'' ) as var_prod_sbtype_cd,
DECODE ( TRUE , var_prod_sbtype_cd IS NULL , ''UNK'' , var_prod_sbtype_cd ) as out_prod_sbtype_cd,
SQ_pcx_etlcovtermpattern.STRT_DT as PROD_STRT_DT,
SQ_pcx_etlcovtermpattern.END_DT as PROD_END_DT,
SQ_pcx_etlcovtermpattern.Retired as Retired,
SQ_pcx_etlcovtermpattern.source_record_id,
row_number() over (partition by SQ_pcx_etlcovtermpattern.source_record_id order by SQ_pcx_etlcovtermpattern.source_record_id) as RNK
FROM
SQ_pcx_etlcovtermpattern

/*Added UPPER function to avoid XLAT value mismatch for joins*/
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_INSRNC_LOB_TYPE LKP_1 ON UPPER(LKP_1.SRC_IDNTFTN_VAL) = UPPER(LTRIM ( RTRIM ( SQ_pcx_etlcovtermpattern.Code ) ))
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT LKP_2 ON UPPER(LKP_2.SRC_IDNTFTN_VAL) = UPPER(LTRIM ( RTRIM ( SQ_pcx_etlcovtermpattern.Code ) ))
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT LKP_3 ON UPPER(LKP_3.SRC_IDNTFTN_VAL) = UPPER(ltrim ( rtrim ( SQ_pcx_etlcovtermpattern.subtype ) ))
QUALIFY RNK = 1
);


-- Component LKP_PROD, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_PROD AS
(
SELECT
LKP.PROD_ID,
LKP.PROD_DESC,
LKP.HOST_PROD_ID,
LKP.PROD_STRT_DTTM,
LKP.PROD_END_DTTM,
LKP.INSRNC_TYPE_CD,
LKP.PRCS_ID,
LKP.INSRNC_LOB_TYPE_CD,
LKP.EDW_STRT_DTTM,
LKP.EDW_END_DTTM,
exp_pass_from_source.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_from_source.source_record_id ORDER BY LKP.PROD_ID asc,LKP.PROD_SCRP_ID asc,LKP.PROD_SBTYPE_CD asc,LKP.PROD_DESC asc,LKP.PROD_NAME asc,LKP.HOST_PROD_ID asc,LKP.PROD_STRT_DTTM asc,LKP.PROD_END_DTTM asc,LKP.PROD_PKG_TYPE_CD asc,LKP.FINCL_PROD_IND asc,LKP.PROD_TXT asc,LKP.PROD_CRTN_DT asc,LKP.INSRNC_TYPE_CD asc,LKP.DY_CNT_BSS_CD asc,LKP.SRY_LVL_CD asc,LKP.CURY_CD asc,LKP.PRCS_ID asc,LKP.INSRNC_LOB_TYPE_CD asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc) RNK
FROM
exp_pass_from_source
LEFT JOIN (
SELECT PROD.PROD_ID as PROD_ID, PROD.PROD_SCRP_ID as PROD_SCRP_ID, PROD.PROD_DESC as PROD_DESC, PROD.HOST_PROD_ID as HOST_PROD_ID, PROD.PROD_STRT_DTTM as PROD_STRT_DTTM, PROD.PROD_END_DTTM as PROD_END_DTTM, PROD.PROD_PKG_TYPE_CD as PROD_PKG_TYPE_CD, PROD.FINCL_PROD_IND as FINCL_PROD_IND, PROD.PROD_TXT as PROD_TXT, PROD.PROD_CRTN_DT as PROD_CRTN_DT, PROD.INSRNC_TYPE_CD as INSRNC_TYPE_CD, PROD.DY_CNT_BSS_CD as DY_CNT_BSS_CD, PROD.SRY_LVL_CD as SRY_LVL_CD, PROD.CURY_CD as CURY_CD, PROD.PRCS_ID as PRCS_ID, PROD.INSRNC_LOB_TYPE_CD as INSRNC_LOB_TYPE_CD, PROD.EDW_STRT_DTTM as EDW_STRT_DTTM, PROD.EDW_END_DTTM as EDW_END_DTTM, PROD.PROD_NAME as PROD_NAME, PROD.PROD_SBTYPE_CD as PROD_SBTYPE_CD FROM DB_T_PROD_CORE.PROD 
QUALIFY ROW_NUMBER() OVER(PARTITION BY PROD.PROD_NAME,PROD.PROD_SBTYPE_CD  ORDER BY PROD.EDW_END_DTTM desc) = 1
) LKP ON LKP.PROD_NAME = exp_pass_from_source.TypeCode AND LKP.PROD_SBTYPE_CD = exp_pass_from_source.out_prod_sbtype_cd /*missing second join condition*/
QUALIFY RNK = 1
);


-- Component exp_ins_upd, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_ins_upd AS
(
SELECT
LKP_PROD.PROD_ID as lkp_PROD_ID,
LKP_PROD.PROD_DESC as lkp_PROD_DESC,
LKP_PROD.HOST_PROD_ID as lkp_HOST_PROD_ID,
LKP_PROD.PROD_STRT_DTTM as lkp_PROD_STRT_DT,
LKP_PROD.PROD_END_DTTM as lkp_PROD_END_DT,
LKP_PROD.INSRNC_TYPE_CD as lkp_INSRNC_TYPE_CD,
LKP_PROD.INSRNC_LOB_TYPE_CD as lkp_INSRNC_LOB_TYPE_CD,
LKP_PROD.EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
LKP_PROD.EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_pass_from_source.TypeCode as TypeCode,
exp_pass_from_source.out_INSRNC_TYPE_CD as out_INSRNC_TYPE_CD,
exp_pass_from_source.Description as Description,
exp_pass_from_source.out_Name as out_Name,
exp_pass_from_source.out_prod_sbtype_cd as out_prod_sbtype_cd,
exp_pass_from_source.ID as ID,
exp_pass_from_source.PRCS_ID as PRCS_ID,
exp_pass_from_source.out_INSRNC_LOB_TYPE_CD as out_INSRNC_LOB_TYPE_CD,
exp_pass_from_source.PROD_STRT_DT as PROD_STRT_DT,
exp_pass_from_source.PROD_END_DT as PROD_END_DT,
exp_pass_from_source.Retired as Retired,
MD5 ( ltrim ( rtrim ( upper ( LKP_PROD.PROD_DESC ) ) ) || ltrim ( rtrim ( LKP_PROD.HOST_PROD_ID ) ) || to_char ( ltrim ( rtrim ( LKP_PROD.PROD_END_DTTM ) ) ) || ltrim ( rtrim ( upper ( LKP_PROD.INSRNC_TYPE_CD ) ) ) || ltrim ( rtrim ( upper ( LKP_PROD.INSRNC_LOB_TYPE_CD ) ) ) ) as v_lkp_chksm,
MD5 ( ltrim ( rtrim ( upper ( exp_pass_from_source.Description ) ) ) || ltrim ( rtrim ( exp_pass_from_source.ID ) ) || to_char ( ltrim ( rtrim ( exp_pass_from_source.PROD_END_DT ) ) ) || ltrim ( rtrim ( upper ( exp_pass_from_source.out_INSRNC_TYPE_CD ) ) ) || ltrim ( rtrim ( upper ( exp_pass_from_source.out_INSRNC_LOB_TYPE_CD ) ) ) ) as v_src_chksm,
CASE WHEN v_lkp_chksm IS NULL THEN ''I'' ELSE CASE WHEN v_lkp_chksm != v_src_chksm THEN ''U'' ELSE ''R'' END END as o_flag,
exp_pass_from_source.source_record_id
FROM
exp_pass_from_source
INNER JOIN LKP_PROD ON exp_pass_from_source.source_record_id = LKP_PROD.source_record_id
);


-- Component rtr_PROD_insert, Type ROUTER Output Group insert
CREATE OR REPLACE TEMPORARY TABLE rtr_PROD_insert AS
(SELECT
exp_ins_upd.TypeCode as TypeCode,
exp_ins_upd.lkp_PROD_ID as PROD_ID,
exp_ins_upd.out_INSRNC_TYPE_CD as INSRNC_TYPE_CD,
exp_ins_upd.Description as PROD_DESC,
exp_ins_upd.out_Name as PROD_NAME,
NULL as PROD_PKG_TYPE_CD,
exp_ins_upd.out_prod_sbtype_cd as PROD_SBTYPE_CD,
exp_ins_upd.ID as HOST_PROD_ID,
exp_ins_upd.PRCS_ID as PRCS_ID,
exp_ins_upd.out_INSRNC_LOB_TYPE_CD as INSRNC_LOB_TYPE_CD,
exp_ins_upd.PROD_STRT_DT as PROD_STRT_DT,
exp_ins_upd.PROD_END_DT as PROD_END_DT,
exp_ins_upd.lkp_PROD_DESC as lkp_PROD_DESC,
exp_ins_upd.lkp_HOST_PROD_ID as lkp_HOST_PROD_ID,
exp_ins_upd.lkp_PROD_STRT_DT as lkp_PROD_STRT_DT,
exp_ins_upd.lkp_PROD_END_DT as lkp_PROD_END_DT,
exp_ins_upd.lkp_INSRNC_TYPE_CD as lkp_INSRNC_TYPE_CD,
exp_ins_upd.lkp_INSRNC_LOB_TYPE_CD as lkp_INSRNC_LOB_TYPE_CD,
exp_ins_upd.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_ins_upd.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_ins_upd.o_flag as o_flag,
exp_ins_upd.Retired as Retired,
exp_ins_upd.source_record_id
FROM
exp_ins_upd
WHERE exp_ins_upd.o_flag = ''I'' OR ( exp_ins_upd.Retired = 0 AND exp_ins_upd.lkp_EDW_END_DTTM != TO_timestamp( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ));


-- Component rtr_PROD_retired, Type ROUTER Output Group retired
CREATE OR REPLACE TEMPORARY TABLE rtr_PROD_retired AS
(SELECT
exp_ins_upd.TypeCode as TypeCode,
exp_ins_upd.lkp_PROD_ID as PROD_ID,
exp_ins_upd.out_INSRNC_TYPE_CD as INSRNC_TYPE_CD,
exp_ins_upd.Description as PROD_DESC,
exp_ins_upd.out_Name as PROD_NAME,
NULL as PROD_PKG_TYPE_CD,
exp_ins_upd.out_prod_sbtype_cd as PROD_SBTYPE_CD,
exp_ins_upd.ID as HOST_PROD_ID,
exp_ins_upd.PRCS_ID as PRCS_ID,
exp_ins_upd.out_INSRNC_LOB_TYPE_CD as INSRNC_LOB_TYPE_CD,
exp_ins_upd.PROD_STRT_DT as PROD_STRT_DT,
exp_ins_upd.PROD_END_DT as PROD_END_DT,
exp_ins_upd.lkp_PROD_DESC as lkp_PROD_DESC,
exp_ins_upd.lkp_HOST_PROD_ID as lkp_HOST_PROD_ID,
exp_ins_upd.lkp_PROD_STRT_DT as lkp_PROD_STRT_DT,
exp_ins_upd.lkp_PROD_END_DT as lkp_PROD_END_DT,
exp_ins_upd.lkp_INSRNC_TYPE_CD as lkp_INSRNC_TYPE_CD,
exp_ins_upd.lkp_INSRNC_LOB_TYPE_CD as lkp_INSRNC_LOB_TYPE_CD,
exp_ins_upd.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_ins_upd.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_ins_upd.o_flag as o_flag,
exp_ins_upd.Retired as Retired,
exp_ins_upd.source_record_id
FROM
exp_ins_upd
WHERE exp_ins_upd.o_flag = ''R'' and exp_ins_upd.Retired != 0 and exp_ins_upd.lkp_EDW_END_DTTM = TO_timestamp ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ));


-- Component rtr_PROD_update, Type ROUTER Output Group update
CREATE OR REPLACE TEMPORARY TABLE rtr_PROD_update AS
(SELECT
exp_ins_upd.TypeCode as TypeCode,
exp_ins_upd.lkp_PROD_ID as PROD_ID,
exp_ins_upd.out_INSRNC_TYPE_CD as INSRNC_TYPE_CD,
exp_ins_upd.Description as PROD_DESC,
exp_ins_upd.out_Name as PROD_NAME,
NULL as PROD_PKG_TYPE_CD,
exp_ins_upd.out_prod_sbtype_cd as PROD_SBTYPE_CD,
exp_ins_upd.ID as HOST_PROD_ID,
exp_ins_upd.PRCS_ID as PRCS_ID,
exp_ins_upd.out_INSRNC_LOB_TYPE_CD as INSRNC_LOB_TYPE_CD,
exp_ins_upd.PROD_STRT_DT as PROD_STRT_DT,
exp_ins_upd.PROD_END_DT as PROD_END_DT,
exp_ins_upd.lkp_PROD_DESC as lkp_PROD_DESC,
exp_ins_upd.lkp_HOST_PROD_ID as lkp_HOST_PROD_ID,
exp_ins_upd.lkp_PROD_STRT_DT as lkp_PROD_STRT_DT,
exp_ins_upd.lkp_PROD_END_DT as lkp_PROD_END_DT,
exp_ins_upd.lkp_INSRNC_TYPE_CD as lkp_INSRNC_TYPE_CD,
exp_ins_upd.lkp_INSRNC_LOB_TYPE_CD as lkp_INSRNC_LOB_TYPE_CD,
exp_ins_upd.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_ins_upd.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_ins_upd.o_flag as o_flag,
exp_ins_upd.Retired as Retired,
exp_ins_upd.source_record_id
FROM
exp_ins_upd
WHERE exp_ins_upd.o_flag = ''U'' AND exp_ins_upd.lkp_EDW_END_DTTM = TO_timestamp ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ));


-- Component upd_prod_insert, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_prod_insert AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_PROD_insert.TypeCode as TypeCode1,
rtr_PROD_insert.INSRNC_TYPE_CD as INSRNC_TYPE_CD,
rtr_PROD_insert.PROD_DESC as PROD_DESC,
rtr_PROD_insert.PROD_NAME as PROD_NAME,
rtr_PROD_insert.PROD_PKG_TYPE_CD as PROD_PKG_TYPE_CD,
rtr_PROD_insert.PROD_SBTYPE_CD as PROD_SBTYPE_CD,
rtr_PROD_insert.HOST_PROD_ID as HOST_PROD_ID,
rtr_PROD_insert.PRCS_ID as PRCS_ID,
rtr_PROD_insert.INSRNC_LOB_TYPE_CD as INSRNC_LOB_TYPE_CD,
rtr_PROD_insert.PROD_STRT_DT as PROD_STRT_DT1,
rtr_PROD_insert.PROD_END_DT as PROD_END_DT1,
rtr_PROD_insert.Retired as Retired1,
0 as UPDATE_STRATEGY_ACTION,
rtr_PROD_insert.source_record_id
FROM
rtr_PROD_insert
);


-- Component upd_prod_retired, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_prod_retired AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_PROD_retired.PROD_ID as PROD_ID4,
rtr_PROD_retired.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM4,
1 as UPDATE_STRATEGY_ACTION,
rtr_PROD_retired.source_record_id
FROM
rtr_PROD_retired
);


-- Component exp_tgt_pass_insert, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_tgt_pass_insert AS
(
SELECT
upd_prod_insert.TypeCode1 as TypeCode1,
upd_prod_insert.INSRNC_TYPE_CD as INSRNC_TYPE_CD,
upd_prod_insert.PROD_DESC as PROD_DESC,
upd_prod_insert.PROD_PKG_TYPE_CD as PROD_PKG_TYPE_CD,
upd_prod_insert.PROD_SBTYPE_CD as PROD_SBTYPE_CD,
upd_prod_insert.HOST_PROD_ID as HOST_PROD_ID,
upd_prod_insert.PRCS_ID as PRCS_ID,
upd_prod_insert.INSRNC_LOB_TYPE_CD as INSRNC_LOB_TYPE_CD,
upd_prod_insert.PROD_STRT_DT1 as PROD_STRT_DT1,
upd_prod_insert.PROD_END_DT1 as PROD_END_DT1,
CURRENT_TIMESTAMP as EDW_STRT_DTTM,
TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as EDW_END_DTTM,
CASE WHEN upd_prod_insert.Retired1 = 0 THEN EDW_END_DTTM ELSE CURRENT_TIMESTAMP END as o_EDW_END_DTTM,
upd_prod_insert.source_record_id
FROM
upd_prod_insert
);


-- Component exp_prod_retired, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_prod_retired AS
(
SELECT
upd_prod_retired.PROD_ID4 as PROD_ID4,
upd_prod_retired.lkp_EDW_STRT_DTTM4 as lkp_EDW_STRT_DTTM4,
CURRENT_TIMESTAMP as EDW_END_DTTM,
upd_prod_retired.source_record_id
FROM
upd_prod_retired
);


-- Component tgt_prod_insert, Type TARGET 
INSERT INTO DB_T_PROD_CORE.PROD
(
PROD_ID,
PROD_SBTYPE_CD,
PROD_DESC,
PROD_NAME,
HOST_PROD_ID,
PROD_STRT_DTTM,
PROD_END_DTTM,
PROD_PKG_TYPE_CD,
INSRNC_TYPE_CD,
INSRNC_LOB_TYPE_CD,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM
)
SELECT
row_number() over (order by 1) as PROD_ID,
exp_tgt_pass_insert.PROD_SBTYPE_CD as PROD_SBTYPE_CD,
exp_tgt_pass_insert.PROD_DESC as PROD_DESC,
exp_tgt_pass_insert.TypeCode1 as PROD_NAME,
exp_tgt_pass_insert.HOST_PROD_ID as HOST_PROD_ID,
exp_tgt_pass_insert.PROD_STRT_DT1 as PROD_STRT_DTTM,
exp_tgt_pass_insert.PROD_END_DT1 as PROD_END_DTTM,
exp_tgt_pass_insert.PROD_PKG_TYPE_CD as PROD_PKG_TYPE_CD,
exp_tgt_pass_insert.INSRNC_TYPE_CD as INSRNC_TYPE_CD,
exp_tgt_pass_insert.INSRNC_LOB_TYPE_CD as INSRNC_LOB_TYPE_CD,
exp_tgt_pass_insert.PRCS_ID as PRCS_ID,
exp_tgt_pass_insert.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_tgt_pass_insert.o_EDW_END_DTTM as EDW_END_DTTM
FROM
exp_tgt_pass_insert;


-- Component upd_prod_update_insert, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_prod_update_insert AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_PROD_update.TypeCode as TypeCode3,
rtr_PROD_update.PROD_ID as PROD_ID_old,
rtr_PROD_update.INSRNC_TYPE_CD as INSRNC_TYPE_CD,
rtr_PROD_update.PROD_DESC as PROD_DESC,
rtr_PROD_update.PROD_NAME as PROD_NAME,
rtr_PROD_update.PROD_PKG_TYPE_CD as PROD_PKG_TYPE_CD,
rtr_PROD_update.PROD_SBTYPE_CD as PROD_SBTYPE_CD,
rtr_PROD_update.HOST_PROD_ID as HOST_PROD_ID,
rtr_PROD_update.PRCS_ID as PRCS_ID,
rtr_PROD_update.INSRNC_LOB_TYPE_CD as INSRNC_LOB_TYPE_CD,
rtr_PROD_update.PROD_STRT_DT as PROD_STRT_DT3,
rtr_PROD_update.PROD_END_DT as PROD_END_DT3,
rtr_PROD_update.Retired as Retired3,
0 as UPDATE_STRATEGY_ACTION,
rtr_PROD_update.SOURCE_RECORD_ID
FROM
rtr_PROD_update
);


-- Component upd_prod_update, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_prod_update AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_PROD_update.TypeCode as TypeCode3,
rtr_PROD_update.PROD_ID as PROD_ID_old,
rtr_PROD_update.INSRNC_TYPE_CD as INSRNC_TYPE_CD,
rtr_PROD_update.PROD_DESC as PROD_DESC,
rtr_PROD_update.PROD_NAME as PROD_NAME,
rtr_PROD_update.PROD_PKG_TYPE_CD as PROD_PKG_TYPE_CD,
rtr_PROD_update.PROD_SBTYPE_CD as PROD_SBTYPE_CD,
rtr_PROD_update.HOST_PROD_ID as HOST_PROD_ID,
rtr_PROD_update.PRCS_ID as PRCS_ID,
rtr_PROD_update.INSRNC_LOB_TYPE_CD as INSRNC_LOB_TYPE_CD,
rtr_PROD_update.PROD_STRT_DT as PROD_STRT_DT3,
rtr_PROD_update.PROD_END_DT as PROD_END_DT3,
rtr_PROD_update.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM3,
1 as UPDATE_STRATEGY_ACTION,
rtr_PROD_update.source_record_id
FROM
rtr_PROD_update
);


-- Component fil_upd_insert, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE fil_upd_insert AS
(
SELECT
upd_prod_update_insert.TypeCode3 as TypeCode3,
upd_prod_update_insert.PROD_ID_old as PROD_ID_old,
upd_prod_update_insert.INSRNC_TYPE_CD as INSRNC_TYPE_CD,
upd_prod_update_insert.PROD_DESC as PROD_DESC,
upd_prod_update_insert.PROD_NAME as PROD_NAME,
upd_prod_update_insert.PROD_PKG_TYPE_CD as PROD_PKG_TYPE_CD,
upd_prod_update_insert.PROD_SBTYPE_CD as PROD_SBTYPE_CD,
upd_prod_update_insert.HOST_PROD_ID as HOST_PROD_ID,
upd_prod_update_insert.PRCS_ID as PRCS_ID,
upd_prod_update_insert.INSRNC_LOB_TYPE_CD as INSRNC_LOB_TYPE_CD,
upd_prod_update_insert.PROD_STRT_DT3 as PROD_STRT_DT3,
upd_prod_update_insert.PROD_END_DT3 as PROD_END_DT3,
upd_prod_update_insert.Retired3 as Retired3,
upd_prod_update_insert.source_record_id
FROM
upd_prod_update_insert
WHERE upd_prod_update_insert.Retired3 = 0
);


-- Component exp_tgt_pass_update, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_tgt_pass_update AS
(
SELECT distinct
upd_prod_update.PROD_ID_old as PROD_ID_old,
upd_prod_update.lkp_EDW_STRT_DTTM3 as lkp_EDW_STRT_DTTM3,
DATEADD (SECOND, -1, CURRENT_TIMESTAMP()) as EDW_END_DTTM,
upd_prod_update.source_record_id
FROM
upd_prod_update
);
-- Component tgt_prod_update, Type TARGET 
MERGE INTO DB_T_PROD_CORE.PROD
USING exp_tgt_pass_update ON (PROD.PROD_ID = exp_tgt_pass_update.PROD_ID_old AND PROD.EDW_STRT_DTTM = exp_tgt_pass_update.lkp_EDW_STRT_DTTM3)
WHEN MATCHED THEN UPDATE
SET
PROD_ID = exp_tgt_pass_update.PROD_ID_old,
EDW_STRT_DTTM = exp_tgt_pass_update.lkp_EDW_STRT_DTTM3,
EDW_END_DTTM = exp_tgt_pass_update.EDW_END_DTTM;


-- Component tgt_prod_retired, Type TARGET 
MERGE INTO DB_T_PROD_CORE.PROD
USING exp_prod_retired ON (PROD.PROD_ID = exp_prod_retired.PROD_ID4 AND PROD.EDW_STRT_DTTM = exp_prod_retired.lkp_EDW_STRT_DTTM4)
WHEN MATCHED THEN UPDATE
SET
PROD_ID = exp_prod_retired.PROD_ID4,
EDW_STRT_DTTM = exp_prod_retired.lkp_EDW_STRT_DTTM4,
EDW_END_DTTM = exp_prod_retired.EDW_END_DTTM;


-- Component exp_tgt_pass_update_insert, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_tgt_pass_update_insert AS
(
SELECT
fil_upd_insert.TypeCode3 as TypeCode3,
fil_upd_insert.PROD_ID_old as PROD_ID_old,
fil_upd_insert.INSRNC_TYPE_CD as INSRNC_TYPE_CD,
fil_upd_insert.PROD_DESC as PROD_DESC,
fil_upd_insert.PROD_SBTYPE_CD as PROD_SBTYPE_CD,
fil_upd_insert.HOST_PROD_ID as HOST_PROD_ID,
fil_upd_insert.PRCS_ID as PRCS_ID,
fil_upd_insert.INSRNC_LOB_TYPE_CD as INSRNC_LOB_TYPE_CD,
fil_upd_insert.PROD_STRT_DT3 as PROD_STRT_DT3,
fil_upd_insert.PROD_END_DT3 as PROD_END_DT3,
CURRENT_TIMESTAMP as EDW_STRT_DTTM,
TO_timestamp ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as EDW_END_DTTM,
CASE WHEN fil_upd_insert.Retired3 != 0 THEN CURRENT_TIMESTAMP ELSE EDW_END_DTTM END as o_EDW_END_DTTM,
fil_upd_insert.source_record_id
FROM
fil_upd_insert
);


-- Component tgt_prod_insert_upd, Type TARGET 
INSERT INTO DB_T_PROD_CORE.PROD
(
PROD_ID,
PROD_SBTYPE_CD,
PROD_DESC,
PROD_NAME,
HOST_PROD_ID,
PROD_STRT_DTTM,
PROD_END_DTTM,
INSRNC_TYPE_CD,
INSRNC_LOB_TYPE_CD,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM
)
SELECT
exp_tgt_pass_update_insert.PROD_ID_old as PROD_ID,
exp_tgt_pass_update_insert.PROD_SBTYPE_CD as PROD_SBTYPE_CD,
exp_tgt_pass_update_insert.PROD_DESC as PROD_DESC,
exp_tgt_pass_update_insert.TypeCode3 as PROD_NAME,
exp_tgt_pass_update_insert.HOST_PROD_ID as HOST_PROD_ID,
exp_tgt_pass_update_insert.PROD_STRT_DT3 as PROD_STRT_DTTM,
exp_tgt_pass_update_insert.PROD_END_DT3 as PROD_END_DTTM,
exp_tgt_pass_update_insert.INSRNC_TYPE_CD as INSRNC_TYPE_CD,
exp_tgt_pass_update_insert.INSRNC_LOB_TYPE_CD as INSRNC_LOB_TYPE_CD,
exp_tgt_pass_update_insert.PRCS_ID as PRCS_ID,
exp_tgt_pass_update_insert.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_tgt_pass_update_insert.o_EDW_END_DTTM as EDW_END_DTTM
FROM
exp_tgt_pass_update_insert;


END; ';