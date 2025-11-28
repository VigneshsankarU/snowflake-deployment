-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_PRTY_TO_PRTY_ASSET_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
       run_id STRING;
       p_load_user string;
       START_DTTM TIMESTAMP;
       END_DTTM TIMESTAMP;
       PRCS_ID STRING;
	   --v_start_time TIMESTAMP;
BEGIN
run_id :=   (SELECT run_id   FROM control_run_id where upper(worklet_name) = upper(:worklet_name) order by insert_ts desc limit 1);   
END_DTTM:=   (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''END_DTTM'' order by insert_ts desc limit 1);
START_DTTM:=     (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''START_DTTM'' order by insert_ts desc limit 1);
PRCS_ID:=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PRCS_ID'' order by insert_ts desc limit 1);
p_load_user :=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''LOAD_USER'' order by insert_ts desc limit 1);
--v_start_time := (select param_value from control_params where run_id = :run_id and upper(param_name)=''START_TIME'' order by insert_ts desc limit 1);

-- Component LKP_BUSN, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_BUSN AS
(
SELECT BUSN.BUSN_PRTY_ID as BUSN_PRTY_ID, BUSN.SRC_SYS_CD as SRC_SYS_CD, BUSN.TAX_BRAKT_CD as TAX_BRAKT_CD, BUSN.ORG_TYPE_CD as ORG_TYPE_CD, BUSN.GICS_SBIDSTRY_CD as GICS_SBIDSTRY_CD, BUSN.LIFCYCL_CD as LIFCYCL_CD, BUSN.PRTY_TYPE_CD as PRTY_TYPE_CD, BUSN.BUSN_END_DTTM as BUSN_END_DTTM, BUSN.BUSN_STRT_DTTM as BUSN_STRT_DTTM, BUSN.INC_IND as INC_IND, BUSN.EDW_STRT_DTTM as EDW_STRT_DTTM, BUSN.EDW_END_DTTM as EDW_END_DTTM, BUSN.BUSN_CTGY_CD as BUSN_CTGY_CD, BUSN.NK_BUSN_CD as NK_BUSN_CD 
FROM db_t_prod_core.BUSN 
QUALIFY ROW_NUMBER () OVER (PARTITION BY NK_BUSN_CD,BUSN_CTGY_CD ORDER BY EDW_END_DTTM DESC )=1
);


-- Component LKP_INDIV_CLM_CTR, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_INDIV_CLM_CTR AS
(
SELECT 
	INDIV.INDIV_PRTY_ID as INDIV_PRTY_ID, 
	INDIV.NK_PUBLC_ID as NK_PUBLC_ID 
FROM 
	db_t_prod_core.INDIV
WHERE
	INDIV.NK_PUBLC_ID IS NOT NULL
);


-- Component LKP_INDIV_CNT_MGR, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_INDIV_CNT_MGR AS
(
SELECT 
	INDIV.INDIV_PRTY_ID as INDIV_PRTY_ID, 
	INDIV.NK_LINK_ID as NK_LINK_ID 
FROM 
	db_t_prod_core.INDIV
WHERE
	INDIV.NK_PUBLC_ID IS NULL
);


-- Component LKP_PRTY_ASSET_ID, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_PRTY_ASSET_ID AS
(
SELECT PRTY_ASSET.PRTY_ASSET_ID as PRTY_ASSET_ID, PRTY_ASSET.ASSET_INSRNC_HIST_TYPE_CD as ASSET_INSRNC_HIST_TYPE_CD, PRTY_ASSET.ASSET_DESC as ASSET_DESC, PRTY_ASSET.PRTY_ASSET_NAME as PRTY_ASSET_NAME, PRTY_ASSET.PRTY_ASSET_STRT_DTTM as PRTY_ASSET_STRT_DTTM, PRTY_ASSET.PRTY_ASSET_END_DTTM as PRTY_ASSET_END_DTTM, PRTY_ASSET.EDW_STRT_DTTM as EDW_STRT_DTTM, PRTY_ASSET.EDW_END_DTTM as EDW_END_DTTM, PRTY_ASSET.SRC_SYS_CD as SRC_SYS_CD, PRTY_ASSET.ASSET_HOST_ID_VAL as ASSET_HOST_ID_VAL, PRTY_ASSET.PRTY_ASSET_SBTYPE_CD as PRTY_ASSET_SBTYPE_CD, PRTY_ASSET.PRTY_ASSET_CLASFCN_CD as PRTY_ASSET_CLASFCN_CD 
FROM db_t_prod_core.PRTY_ASSET 
QUALIFY ROW_NUMBER() OVER(PARTITION BY  ASSET_HOST_ID_VAL,PRTY_ASSET_SBTYPE_CD,PRTY_ASSET_CLASFCN_CD ORDER BY EDW_END_DTTM DESC) = 1
);


-- Component LKP_PRTY_TO_PRTY_ASSET, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_PRTY_TO_PRTY_ASSET AS
(
SELECT
PRTY_ASSET_ID,
ASSET_ROLE_CD,
ASSET_ROLE_DTTM,
PRTY_ID
FROM db_t_prod_core.PRTY_TO_PRTY_ASSET
);


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

             		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS in (''DS'', ''GW'') 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_ASSET_ROLE_CD, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_ASSET_ROLE_CD AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL AS TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL AS SRC_IDNTFTN_VAL 

FROM 

	db_t_prod_core.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''ASSET_ROLE_TYPE'' AND 

	TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
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


-- Component LKP_TERADATA_ETL_REF_XLAT_ASSET_USE_CD, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_ASSET_USE_CD AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL AS TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL AS SRC_IDNTFTN_VAL 

FROM 

	db_t_prod_core.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''ASSET_USE_TYPE''  AND 

		TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''GW'' AND 

	TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_BUSN_CTGY, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_BUSN_CTGY AS
(
SELECT 

 TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

 ,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

 db_t_prod_core.TERADATA_ETL_REF_XLAT

WHERE 

 TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM IN (''BUSN_CTGY'',''ORG_TYPE'',''PRTY_TYPE'')

 AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM IN (''derived'', ''cctl_contact.typecode'',''cctl_contact.name'')

 AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS IN (''DS'',''GW'')

 AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_LIENHOLDER_CD, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_LIENHOLDER_CD AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	db_t_prod_core.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''LIENHLDR_POSITN_TYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''pctl_mortgageetype_alfa.typecode'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''GW'' 

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

-- Component SQ_cc_prty_to_prty_asset, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_cc_prty_to_prty_asset AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as PolicySystemId,
$2 as sbtype,
$3 as classification_code,
$4 as src_cd,
$5 as typecode,
$6 as role_cd,
$7 as role_dt,
$8 as addressbookuid,
$9 as CTL_ID,
$10 as LOAD_USER,
$11 as RateDriverclassalfa,
$12 as creationdate,
$13 as expirationdate,
$14 as UPDATETIME,
$15 as mortgageetype_alfa,
$16 as Retired,
$17 as typecode1,
$18 as Rnk,
$19 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT DISTINCT cc_prty_to_prty_asset.PolicySystemId, cc_prty_to_prty_asset.sbtype, cc_prty_to_prty_asset.classification_code, 

cc_prty_to_prty_asset.src_cd, cc_prty_to_prty_asset.typecode, cc_prty_to_prty_asset.role_cd, cc_prty_to_prty_asset.role_dt, 

substr(cc_prty_to_prty_asset.addressbookuid,1,regexp_instr(cc_prty_to_prty_asset.addressbookuid,''-'',1)-1) as addressbookuid, 

cc_prty_to_prty_asset.CTL_ID, cc_prty_to_prty_asset.LOAD_USER, cc_prty_to_prty_asset.RateDriverclassalfa, 

cc_prty_to_prty_asset.creationdate, cc_prty_to_prty_asset.expirationdate, cc_prty_to_prty_asset.UPDATETIME, cc_prty_to_prty_asset.mortgageetype_alfa, cc_prty_to_prty_asset.Retired,

substr(cc_prty_to_prty_asset.addressbookuid,regexp_instr(cc_prty_to_prty_asset.addressbookuid,''-'',1)+1) as typecode1,cast(1 as number(30) ) as rnk

FROM

(

SELECT * FROM(

SELECT cast(PolicySystemId as varchar(100))as PolicySystemId, sbtype, CLASSIFICATION_CODE, SRC_CD, role_cd, role_dt,TYPECODE, addressbookuid, updatetime, creationdate,

expirationdate,RateDriverclassalfa,mortgageetype_alfa,retired, CTL_ID,LOAD_USER  from 

(SELECT distinct

pc_personalvehicle.FixedID_stg as PolicySystemId,

 cast(''PRTY_ASSET_SBTYPE4'' as varchar(100))AS sbtype, 

 cast(''PRTY_ASSET_CLASFCN3'' as varchar(100))as  classification_code,

 cast(''GWPC'' as varchar(100))as src_cd, 

 cast(''ASSET_ROLE_TYPE12''as varchar(100)) AS role_cd,

 cast(case when  pc_personalvehicle.PurchaseDate_Alfa_stg is null then cast(''1900-01-01 00:00:00.000000'' AS TIMESTAMP(6)) else pc_personalvehicle.PurchaseDate_Alfa_stg end as timestamp(6)) AS role_dt,

 pctl_contact.TYPECODE_stg as typecode,

 coalesce(pc_contact.addressbookuid_stg,'''')||''-''||pctl_contact.TYPECODE_stg AS addressbookuid,

 cast(''1'' as SMALLINT) as CTL_ID,

 (:p_load_user) as LOAD_USER,

 pc_personalvehicle.UpdateTime_stg as UPDATETIME,

 pc_personalvehicle.CreateTime_stg as creationdate,

 cast(case when pc_personalvehicle.ExpirationDate_stg is null then cast(''1900-01-01'' as date  ) else 

 pc_personalvehicle.ExpirationDate_stg end as TIMESTAMP(6))as expirationdate,

pc_personalvehicle.RateDriverClass_alfa_stg as RateDriverclassalfa,

cast(null as varchar(100))as mortgageetype_alfa,

case when (pc_policyperiod.Retired_stg=0 and pc_contact.Retired_stg=0) then 0 else 1 end as Retired,

row_number () over(partition by pc_personalvehicle.fixedid_stg order by TERMNUMBER_stg DESC,ModelNumber_stg desc ) rw

FROM  DB_T_PROD_STAG.pc_personalvehicle 

JOIN  DB_T_PROD_STAG.pc_policyperiod ON pc_policyperiod.id_stg = pc_personalvehicle.branchid_stg 

JOIN DB_T_PROD_STAG.pc_policycontactrole ON pc_policyperiod.id_stg = pc_policycontactrole.branchid_stg 

JOIN DB_T_PROD_STAG.pc_contact ON pc_contact.ID_stg = pc_policycontactrole.ContactDenorm_stg 

JOIN DB_T_PROD_STAG.pctl_contact on pctl_contact.id_stg=pc_contact.Subtype_stg 

JOIN DB_T_PROD_STAG.pctl_policycontactrole ON pc_policycontactrole.Subtype_stg = pctl_policycontactrole.ID_stg AND pctl_policycontactrole.name_stg = ''PolicyPriNamedInsured''

where pc_personalvehicle.ExpirationDate_stg is null AND(

(pc_personalvehicle.updatetime_stg > (:START_DTTM) and pc_personalvehicle.updatetime_stg <= (:END_DTTM))

OR (pc_policyperiod.updatetime_stg > (:START_DTTM) and pc_policyperiod.updatetime_stg <= (:END_DTTM))

OR (pc_policycontactrole.updatetime_stg > (:START_DTTM) and pc_policycontactrole.updatetime_stg <= (:END_DTTM))

OR (pc_contact.updatetime_stg > (:START_DTTM) and pc_contact.updatetime_stg <= (:END_DTTM)))

)A WHERE RW=1 



UNION



/** Dwelling Purchased Date by Owner**/



SELECT distinct PolicySystemId, sbtype, classification_code, src_cd, role_cd, cast(case when role_dt is null then cast(''1900-01-01 00:00:00.000000'' AS TIMESTAMP(6)) else role_dt end as TIMESTAMP(6))role_dt,

typecode, addressbookuid, UpdateTime, creationdate, Expirationdate, RateDriverclassalfa, mortgageetype_alfa, retired, CTL_ID, LOAD_USER FROM (

SELECT distinct

cast(pcx_dwelling_hoe.FixedID_stg as varchar(100)) as PolicySystemId, 

cast(''PRTY_ASSET_SBTYPE5'' as varchar(100)) AS sbtype, 

cast(''PRTY_ASSET_CLASFCN1'' as varchar(100))as classification_code,

cast(''GWPC'' as varchar(100)) as src_cd, 

 cast(''ASSET_ROLE_TYPE12'' as varchar(100)) AS role_cd,

 cast(cast(cast(YearPurchased_stg as varchar(100))||''-''||trim(cast(

		case 

			when pcx_dwelling_hoe.MonthPurchased_alfa_stg in (''1'',''2'',''3'',

		''4'',''5'',''6'',''7'',''8'',''9'') then 0||trim(pcx_dwelling_hoe.MonthPurchased_alfa_stg) 

			else pcx_dwelling_hoe.MonthPurchased_alfa_stg 

		end as varchar(100))||''-''||''01'') as date)as  timestamp(6))role_dt,

pctl_contact.TYPECODE_stg as typecode,

coalesce(pc_contact.addressbookuid_stg,'''')||''-''||pctl_contact.TYPECODE_stg as  addressbookuid,

pcx_dwelling_hoe.UpdateTime_stg as UpdateTime,

cast(''1900-01-01 00:00:00.000000'' as TIMESTAMP(6)) as creationdate,

cast(''1900-01-01 00:00:00.000000'' as TIMESTAMP(6)) as Expirationdate,

cast(NULL as varchar(100)) RateDriverclassalfa,

cast(null as varchar(100)) as mortgageetype_alfa,

case when (pc_policyperiod.Retired_stg=0 and pc_contact.Retired_stg=0) then 0 else 1 end as retired,

cast(''1'' as SMALLINT) as CTL_ID,

(:p_load_user) as LOAD_USER

FROM  DB_T_PROD_STAG.pcx_dwelling_hoe 

JOIN  DB_T_PROD_STAG.pc_policyperiod ON pc_policyperiod.id_stg = pcx_dwelling_hoe.branchid_stg 

JOIN DB_T_PROD_STAG.pc_policycontactrole ON pc_policyperiod.id_stg = pc_policycontactrole.branchid_stg 

JOIN DB_T_PROD_STAG.pc_contact ON pc_contact.ID_stg = pc_policycontactrole.ContactDenorm_stg 

JOIN DB_T_PROD_STAG.pctl_contact on pctl_contact.id_stg=pc_contact.Subtype_stg 

JOIN DB_T_PROD_STAG.pctl_policycontactrole ON pc_policycontactrole.Subtype_stg = pctl_policycontactrole.ID_stg AND pctl_policycontactrole.name_stg = ''PolicyPriNamedInsured'' 

where  pcx_dwelling_hoe.ExpirationDate_stg is null/*EIM 13678*/ 

AND (

 (pcx_dwelling_hoe.updatetime_stg > (:START_DTTM) and pcx_dwelling_hoe.updatetime_stg <= (:END_DTTM))

OR (pc_policyperiod.updatetime_stg > (:START_DTTM) and pc_policyperiod.updatetime_stg <= (:END_DTTM))

OR (pc_policycontactrole.updatetime_stg > (:START_DTTM) and pc_policycontactrole.updatetime_stg <= (:END_DTTM))

OR (pc_contact.updatetime_stg > (:START_DTTM) and pc_contact.updatetime_stg <= (:END_DTTM)))



/*EIM-49111 FARM CHANGES*/

UNION

SELECT distinct

cast(pcx_fopdwelling.FixedID_stg as varchar(100)) as PolicySystemId, 

cast(''PRTY_ASSET_SBTYPE37'' as varchar(100)) AS sbtype, 

cast(''PRTY_ASSET_CLASFCN15'' as varchar(100))as classification_code,

cast(''GWPC'' as varchar(50)) as src_cd, 

 cast(''ASSET_ROLE_TYPE12'' as varchar(100)) AS role_cd,

 cast(cast(cast(YearPurchased_stg as varchar(100))||''-''||trim(cast(

        case 

            when pcx_fopdwelling.MonthPurchased_stg in (''1'',''2'',''3'',

        ''4'',''5'',''6'',''7'',''8'',''9'') then 0||trim(pcx_fopdwelling.MonthPurchased_stg) 

            else pcx_fopdwelling.MonthPurchased_stg 

        end as varchar(100))||''-''||''01'') as date)as  timestamp(6))role_dt,

pctl_contact.TYPECODE_stg as typecode,

coalesce(pc_contact.addressbookuid_stg,'''')||''-''||pctl_contact.TYPECODE_stg as  addressbookuid,

pcx_fopdwelling.UpdateTime_stg as UpdateTime,

cast(''1900-01-01 00:00:00.000000'' as TIMESTAMP(6)) as creationdate,

cast(''1900-01-01 00:00:00.000000'' as TIMESTAMP(6)) as Expirationdate,

cast(NULL as varchar(50)) RateDriverclassalfa,

cast(null as varchar(50)) as mortgageetype_alfa,

case when (pc_policyperiod.Retired_stg=0 and pc_contact.Retired_stg=0) then 0 else 1 end as retired,

cast(''1'' as SMALLINT) as CTL_ID,

(:p_load_user) as LOAD_USER

FROM  DB_T_PROD_STAG.pcx_fopdwelling 

JOIN  DB_T_PROD_STAG.pc_policyperiod ON pc_policyperiod.id_stg = pcx_fopdwelling.branchid_stg 

JOIN DB_T_PROD_STAG.pc_policycontactrole ON pc_policyperiod.id_stg = pc_policycontactrole.branchid_stg 

JOIN DB_T_PROD_STAG.pc_contact ON pc_contact.ID_stg = pc_policycontactrole.ContactDenorm_stg 

JOIN DB_T_PROD_STAG.pctl_contact on pctl_contact.id_stg=pc_contact.Subtype_stg 

JOIN DB_T_PROD_STAG.pctl_policycontactrole ON pc_policycontactrole.Subtype_stg = pctl_policycontactrole.ID_stg AND pctl_policycontactrole.name_stg = ''PolicyPriNamedInsured'' 

where (  pcx_fopdwelling.ExpirationDate_stg is null or  pcx_fopdwelling.ExpirationDate_stg>EditeffectiveDate_stg)

AND (

 (pcx_fopdwelling.updatetime_stg > (:START_DTTM) and pcx_fopdwelling.updatetime_stg <= (:END_DTTM))

OR (pc_policyperiod.updatetime_stg > (:START_DTTM) and pc_policyperiod.updatetime_stg <= (:END_DTTM))

OR (pc_policycontactrole.updatetime_stg > (:START_DTTM) and pc_policycontactrole.updatetime_stg <= (:END_DTTM))

OR (pc_contact.updatetime_stg > (:START_DTTM) and pc_contact.updatetime_stg <= (:END_DTTM)))

Qualify row_number() over(partition by pcx_fopdwelling.FixedID_stg order by coalesce(pcx_fopdwelling.expirationdate_stg, cast(''9999-12-31 23:59:59.999999'' as timestamp(6))) desc,pcx_fopdwelling.createtime_stg desc,pcx_fopdwelling.updatetime_stg desc)=1

) X

/*EIM-49111*/UNION



select distinct 

cast(pc_personalvehicle.FixedID_stg as varchar(100))as  PolicySystemId,

cast(''PRTY_ASSET_SBTYPE4'' as varchar(100)) as sbtype, 

cast(''PRTY_ASSET_CLASFCN3'' as varchar(100))as classification_code,

cast(''GWPC'' as varchar(100))as src_cd,

pctl_additionalinteresttype.TYPECODE_stg as  role_cd,

cast(case when a.UpdateTime_stg is null then cast(''1900-01-01 00:00:00.000000'' AS TIMESTAMP(6)) else a.UpdateTime_stg end as timestamp(6))as role_dt,

pctl_contact.TYPECODE_stg as  typecode,

coalesce(g.AddressBookUID_stg,'''')||''-''||pctl_contact.TYPECODE_stg as  addressbookuid,

a.updatetime_stg as UpdateTime,

cast(''1900-01-01 00:00:00.000000'' as TIMESTAMP(6)) as creationdate,

cast(''1900-01-01 00:00:00.000000'' as TIMESTAMP(6)) as Expirationdate,

pc_personalvehicle.RateDriverClass_alfa_stg as RateDriverclassalfa,

pctl_mortgageetype_alfa.TYPECODE_stg as mortgageetype_alfa,

case when (d.Retired_stg=0 and f.Retired_stg=0 and g.Retired_stg=0) then 0 else 1 end as retired,

cast(''1'' as SMALLINT) as CTL_ID,

(:p_load_user) as LOAD_USER

from DB_T_PROD_STAG.pc_addlinterestdetail a 

inner join DB_T_PROD_STAG.pc_personalvehicle on a.PAVehicle_stg=pc_personalvehicle.id_stg

inner join DB_T_PROD_STAG.pc_policycontactrole b on b.id_stg = a.PolicyAddlInterest_stg

inner join DB_T_PROD_STAG.pctl_policycontactrole c on c.id_stg = b.Subtype_stg

inner join DB_T_PROD_STAG.pc_accountcontactrole d on d.id_stg = b.AccountContactRole_stg

inner join DB_T_PROD_STAG.pctl_accountcontactrole e on e.id_stg = d.Subtype_stg

inner join DB_T_PROD_STAG.pc_accountcontact f on f.id_stg = d.AccountContact_stg

inner join DB_T_PROD_STAG.pc_contact g on g.id_stg = f.Contact_stg

inner join DB_T_PROD_STAG.pctl_additionalinteresttype on pctl_additionalinteresttype.id_stg=a.AdditionalInterestType_stg

inner join DB_T_PROD_STAG.pctl_contact on pctl_contact.id_stg=g.Subtype_stg

left outer join DB_T_PROD_STAG.pctl_mortgageetype_alfa on a.MortgageeType_alfa_stg=pctl_mortgageetype_alfa.ID_stg

where e.TYPECODE_stg = ''AdditionalInterest'' and pctl_additionalinteresttype.TYPECODE_stg not in (''LHFirst_alfa'') 

AND (

(a.updatetime_stg > (:START_DTTM) and a.updatetime_stg <= (:END_DTTM))

OR (pc_personalvehicle.updatetime_stg > (:START_DTTM) and pc_personalvehicle.updatetime_stg <= (:END_DTTM))

OR (b.updatetime_stg > (:START_DTTM) and b.updatetime_stg <= (:END_DTTM))

OR (d.updatetime_stg > (:START_DTTM) and d.updatetime_stg <= (:END_DTTM))

OR (f.updatetime_stg > (:START_DTTM) and f.updatetime_stg <= (:END_DTTM))

OR (g.updatetime_stg > (:START_DTTM) and g.updatetime_stg <= (:END_DTTM)))



UNION



/** Lienholder Info for Dwelling**/

select distinct 

cast(pcx_dwelling_hoe.FixedID_stg as varchar(100))as PolicySystemId ,

cast(''PRTY_ASSET_SBTYPE5'' as varchar(100)) AS sbtype,

cast(''PRTY_ASSET_CLASFCN1'' as varchar(100))as classification_code,

cast(''GWPC''as varchar(100)) as src_cd, 

pctl_additionalinteresttype.TYPECODE_stg as role_cd,

CAST(case when a.UpdateTime_stg is null then cast(''1900-01-01 00:00:00.000000'' AS TIMESTAMP(6)) else a.UpdateTime_stg end as TIMESTAMP(6))as role_dt,

pctl_contact.TYPECODE_stg as typecode,

coalesce(g.AddressBookUID_stg,'''')||''-''||pctl_contact.TYPECODE_stg as addressbookuid,

a.updatetime_stg as updatetime,

cast(''1900-01-01 00:00:00.000000'' as TIMESTAMP(6)) as creationdate,

cast(''1900-01-01 00:00:00.000000'' as TIMESTAMP(6)) as Expirationdate,

cast(NULL as varchar(100))as RateDriverclassalfa,

pctl_mortgageetype_alfa.TYPECODE_stg as mortgageetype_alfa,

case when (d.Retired_stg=0 and f.Retired_stg=0 and g.Retired_stg=0) then 0 else 1 end as retired,

cast(''1'' as SMALLINT) as CTL_ID,

(:p_load_user) as LOAD_USER

from DB_T_PROD_STAG.pc_addlinterestdetail a 

inner join DB_T_PROD_STAG.pcx_dwelling_hoe on a.Dwelling_stg=pcx_dwelling_hoe.id_stg

inner join DB_T_PROD_STAG.pc_policycontactrole b on b.id_stg = a.PolicyAddlInterest_stg

inner join DB_T_PROD_STAG.pctl_policycontactrole c on c.id_stg = b.Subtype_stg

inner join DB_T_PROD_STAG.pc_accountcontactrole d on d.id_stg = b.AccountContactRole_stg

inner join DB_T_PROD_STAG.pctl_accountcontactrole e on e.id_stg = d.Subtype_stg

inner join DB_T_PROD_STAG.pc_accountcontact f on f.id_stg = d.AccountContact_stg

inner join DB_T_PROD_STAG.pc_contact g on g.id_stg = f.Contact_stg

inner join DB_T_PROD_STAG.pctl_additionalinteresttype on pctl_additionalinteresttype.id_stg=a.AdditionalInterestType_stg

inner join DB_T_PROD_STAG.pctl_contact on pctl_contact.id_stg=g.Subtype_stg

left outer join DB_T_PROD_STAG.pctl_mortgageetype_alfa on a.MortgageeType_alfa_stg=pctl_mortgageetype_alfa.ID_stg

where e.TYPECODE_stg = ''AdditionalInterest''

AND (

(a.updatetime_stg > (:START_DTTM) and a.updatetime_stg <= (:END_DTTM))

OR (pcx_dwelling_hoe.updatetime_stg > (:START_DTTM) and pcx_dwelling_hoe.updatetime_stg <= (:END_DTTM))

OR (b.updatetime_stg > (:START_DTTM) and b.updatetime_stg <= (:END_DTTM))

OR (d.updatetime_stg > (:START_DTTM) and d.updatetime_stg <= (:END_DTTM))

OR (f.updatetime_stg > (:START_DTTM) and f.updatetime_stg <= (:END_DTTM))

OR (g.updatetime_stg > (:START_DTTM) and g.updatetime_stg <= (:END_DTTM))

))A

union all



SELECT * FROM (

SELECT DISTINCT cast(id as varchar(100))as PolicySystemId, sbtype, classification_code, src_cd, role_cd, CAST(case when roledate is null then cast(''1900-01-01 00:00:00.000000'' AS TIMESTAMP(6)) else roledate end as TIMESTAMP(6))as role_dt, TYPECODE, addressbookuid, updatetime, 

creationdate, expirationdate, RateDriverclassalfa, mortgageetype_alfa,retired, CTL_ID, LOAD_USER FROM( 

select distinct  id,sbtype,classification_code,src_cd,role_cd,

(case when (cast(lag_exp as date)=cast(''9999-12-31'' as date)) then coalesce(lag_eff,roledt,min(periodstart) over (partition by id,addressbookuid order by termnumber,modelnumber,updatetime)) else coalesce(roledt,lag_eff,periodstart) end) roledate,

TYPECODE,

addressbookuid,

updatetime,

cast(creationdate as timestamp(6))as creationdate , 

cast(expirationdate as timestamp(6))as expirationdate,

RateDriverclassalfa,

mortgageetype_alfa,

retired,cast(''1'' as SMALLINT) as CTL_ID,

(:p_load_user) as LOAD_USER from

(

select distinct id,sbtype,addressbookuid,classification_code,src_cd,role_cd,roledt,updatetime,TYPECODE,expirationdate,periodstart,effectivedate,

lag(roledt) over(partition by id,addressbookuid order by termnumber,modelnumber,updatetime) lag_eff, lag_exp,

creationdate, RateDriverclassalfa,

mortgageetype_alfa,

retired,start_dttm,end_dttm,termnumber,modelnumber

from

(

select distinct

id,sbtype,classification_code,src_cd,role_cd,periodstart,effectivedate,

(case when effectivedate is null then (lag(effectivedate) over(partition by id,addressbookuid order by termnumber,modelnumber,updatetime)) else effectivedate end) roledt,

TYPECODE,addressbookuid,updatetime,creationdate,

expirationdate, lag(expirationdate) over (order by id,updatetime)lag_exp,

RateDriverclassalfa,

mortgageetype_alfa,

retired,start_dttm,end_dttm,termnumber,modelnumber,rnk

from

(

select distinct id,

cast(''PRTY_ASSET_SBTYPE4'' as varchar(100)) as sbtype ,

cast(''PRTY_ASSET_CLASFCN3'' as varchar(100)) as classification_code,

cast(''GWPC'' as varchar(100))as src_cd,

 termnumber, modelnumber, addressbookuid,TYPECODE,role_cd,mortgageetype_alfa,

(case when (effectivedate is null and rnk=1 ) then periodstart else effectivedate end) as effectivedate, 

(case when (expirationdate is null) then cast(''9999-12-31 00:00:00.000000'' as TIMESTAMP(6))  else expirationdate end) as expirationdate,

updatetime, RateDriverclassalfa,

cast(''1900-01-01 00:00:00.000000'' as TIMESTAMP(6)) as creationdate,

cast (''1900-01-01 00:00:00.000000'' as TIMESTAMP(6)) as start_dttm,

cast (''9999-01-01 00:00:00.000000'' as TIMESTAMP(6)) as end_dttm,

periodstart,periodend,retired,rnk from

(

select distinct  id, termnumber, modelnumber, updatetime, RateDriverclassalfa,

effectivedate, expirationdate,addressbookuid,TYPECODE,role_cd, mortgageetype_alfa,

  periodstart, periodend,retired,

rank() over(partition by id,addressbookuid order by termnumber,modelnumber,updatetime) rnk from

(

select distinct pc_personalvehicle.FixedID_stg as id, 

pol.termnumber_stg as termnumber,

pol.modelnumber_stg as  modelnumber, addl.updatetime_stg as  updatetime, pc_personalvehicle.RateDriverClass_alfa_stg as  RateDriverclassalfa,

addl.effectivedate_stg as  effectivedate, pctl_contact.TYPECODE_stg as TYPECODE,pctl_additionalinteresttype.TYPECODE_stg as role_cd,

addl.expirationdate_stg as expirationdate, g.AddressBookUID_stg||''-''||pctl_contact.TYPECODE_stg as addressbookuid, pctl_mortgageetype_alfa.TYPECODE_stg as mortgageetype_alfa, case when (d.Retired_stg=0 and f.Retired_stg=0 and g.Retired_stg=0) then 0 else 1 end as retired,

pol.periodStart_stg as periodstart,

pol.periodEnd_stg as periodend

from DB_T_PROD_STAG.pc_addlinterestdetail addl 

inner JOIN  DB_T_PROD_STAG.pc_policyperiod pol ON  pol.id_stg=addl.BranchID_stg

inner join DB_T_PROD_STAG.pc_personalvehicle on addl.PAVehicle_stg=pc_personalvehicle.id_stg

inner join DB_T_PROD_STAG.pc_policycontactrole b on b.id_stg = addl.PolicyAddlInterest_stg

inner join DB_T_PROD_STAG.pctl_policycontactrole c on c.id_stg = b.Subtype_stg

inner join DB_T_PROD_STAG.pc_accountcontactrole d on d.id_stg = b.AccountContactRole_stg

inner join DB_T_PROD_STAG.pctl_accountcontactrole e on e.id_Stg = d.Subtype_stg

inner join DB_T_PROD_STAG.pc_accountcontact f on f.id_stg = d.AccountContact_stg

inner join DB_T_PROD_STAG.pc_contact g on g.id_stg = f.Contact_stg

inner join DB_T_PROD_STAG.pctl_additionalinteresttype on pctl_additionalinteresttype.id_stg=addl.AdditionalInterestType_stg

inner join DB_T_PROD_STAG.pctl_contact on pctl_contact.id_stg=g.Subtype_stg

left outer join DB_T_PROD_STAG.pctl_mortgageetype_alfa on addl.MortgageeType_alfa_stg=pctl_mortgageetype_alfa.ID_stg

where

e.TYPECODE_stg = ''AdditionalInterest''  

and pctl_additionalinteresttype.TYPECODE_stg in (''LHFirst_alfa'') 

AND (

(addl.updatetime_stg > (:START_DTTM) and addl.updatetime_stg <= (:END_DTTM))

OR (pol.updatetime_stg > (:START_DTTM) and pol.updatetime_stg <= (:END_DTTM))

OR (pc_personalvehicle.updatetime_stg > (:START_DTTM) and pc_personalvehicle.updatetime_stg <= (:END_DTTM))

OR (b.updatetime_stg > (:START_DTTM) and b.updatetime_stg <= (:END_DTTM))

OR (d.updatetime_stg > (:START_DTTM) and d.updatetime_stg <= (:END_DTTM))

OR (f.updatetime_stg > (:START_DTTM) and f.updatetime_stg <= (:END_DTTM))

OR (g.updatetime_stg > (:START_DTTM) and g.updatetime_stg <= (:END_DTTM))

)

    ) innr_mst

       )rnk

   )lag_inr

   )lag_col

    )final)X)B

	

	union all

	

	SELECT * FROM(

select distinct 

case when cc_vehicle.PolicySystemId_stg is not null then SUBSTR(cc_vehicle.policysystemid_stg, POSITION('':''in cc_vehicle.policysystemid_stg)+1,LENGTH(cc_vehicle.policysystemid_stg))

when (cc_vehicle.PolicySystemId_stg is null and cc_vehicle.Vin_stg is not null) then  ''VIN:''||cc_vehicle.vin_stg 

when (cc_vehicle.PolicySystemId_stg is null and cc_vehicle.Vin_stg is null and cc_vehicle.LicensePlate_stg is not null) then ''LP:''||cc_vehicle.licenseplate_stg

when (cc_vehicle.PolicySystemId_stg is null and cc_vehicle.Vin_stg is null and cc_vehicle.LicensePlate_stg is null) then cc_vehicle.PublicID_stg

end as PolicySystemId,

cast(''PRTY_ASSET_SBTYPE4'' as varchar(100)) as sbtype ,

cast(''PRTY_ASSET_CLASFCN3'' as varchar(100)) as classification_code,

case when PolicySystemId_stg is null then cast(''GWCC'' as varchar(100))else cast(''GWPC'' as varchar(100)) end as src_cd,

cast(''ASSET_ROLE_TYPE9'' as varchar(100)) as role_cd,

cast(case when cc_incident.DateSalvageAssigned_stg is null then cast(''1900-01-01 00:00:00.000000'' AS TIMESTAMP(6)) else cc_incident.DateSalvageAssigned_stg end  as TIMESTAMP(6))as role_dt,

cast(''BUSN_CTGY5'' as varchar(100))as typecode,

coalesce(cctl_salvageyard_alfa.TYPECODE_stg,'''')||''-''||cast(''BUSN_CTGY5'' as varchar(100))as addressbookuid,

cc_incident.updatetime_stg as UPDATETIME,

cast(''1900-01-01 00:00:00.000000'' as TIMESTAMP(6)) as creationdate,

cast(''1900-01-01 00:00:00.000000'' as TIMESTAMP(6)) as Expirationdate,

cast(NULL as varchar(100)) as  RateDriverclassalfa,

cast(null as varchar(100)) as mortgageetype_alfa,

case when (cc_incident.Retired_stg=0 and cc_vehicle.Retired_stg=0 ) 

then 0 else 1 end  as retired,

cast(''1'' as SMALLINT) as CTL_ID,

(:p_load_user) as LOAD_USER

from DB_T_PROD_STAG.cc_incident 

inner join DB_T_PROD_STAG.cc_vehicle on cc_incident.VehicleID_stg = cc_vehicle.ID_stg

left outer join DB_T_PROD_STAG.cctl_incident on cc_incident.Subtype_stg = cctl_incident.id_stg

left join DB_T_PROD_STAG.cctl_salvageyard_alfa on cctl_salvageyard_alfa.id_stg=cc_incident.SalvageYard_alfa_stg

where

(cc_incident.updatetime_stg > (:START_DTTM) and cc_incident.updatetime_stg <= (:END_DTTM))

OR (cc_vehicle.updatetime_stg > (:START_DTTM) and cc_vehicle.updatetime_stg <= (:END_DTTM))





Union



select distinct 

/** Party Asset **/

case when (cc_vehicle.PolicySystemId_stg is null and cc_vehicle.Vin_stg is not null) then  ''VIN:''||cc_vehicle.vin_stg 

when (cc_vehicle.PolicySystemId_stg is null and cc_vehicle.Vin_stg is null and cc_vehicle.LicensePlate_stg is not null) then ''LP:''||cc_vehicle.licenseplate_stg

when (cc_vehicle.PolicySystemId_stg is null and cc_vehicle.Vin_stg is null and cc_vehicle.LicensePlate_stg is null) then cc_vehicle.PublicID_stg

end as PolicySystemId,

cast(''PRTY_ASSET_SBTYPE4'' as varchar(100))as sbtype,

cast(''PRTY_ASSET_CLASFCN3'' as  varchar(100))as classification_code,

case when cc_vehicle.PolicySystemId_stg is null then cast(''GWCC''as varchar(100)) else cast(''GWPC'' as varchar(100)) end as src_cd,

cctl_vehicletype.typecode_stg as role_cd,

cast(case when cc_Claim.LossDate_stg is null then cast(''1900-01-01 00:00:00.000000'' AS TIMESTAMP(6)) else cc_Claim.LossDate_stg end as TIMESTAMP(6))as role_dt, 

cctl_reasonforuse.TYPECODE_stg  as TYPECODE,

coalesce(cc_contact.PublicID_stg,'''')||''-''||cctl_contact.typecode_stg as addressbookuid, 

cc_claimcontactrole.UpdateTime_stg as UpdateTime,

cast(''1900-01-01 00:00:00.000000'' as TIMESTAMP(6)) as creationdate,

cast(''1900-01-01 00:00:00.000000'' as TIMESTAMP(6)) as Expirationdate,

cast(NULL as varchar(100))as RateDriverclassalfa,

cast(null as varchar(100)) as mortgageetype_alfa,

case when (cc_incident.Retired_stg=0 and cc_vehicle.Retired_stg =0 

and cc_claim.Retired_stg=0 and cc_claimcontactrole.Retired_stg=0 and cc_claimcontact.Retired_stg=0 

and cc_contact.Retired_stg=0 ) 

then 0 else 1 end as retired,

cast(''1'' as SMALLINT) as CTL_ID,

(:p_load_user) as LOAD_USER

FROM DB_T_PROD_STAG.cc_incident 

inner join DB_T_PROD_STAG.cc_vehicle on cc_incident.VehicleID_stg = cc_vehicle.ID_stg

left outer join DB_T_PROD_STAG.cctl_incident on cc_incident.Subtype_stg = cctl_incident.id_stg

LEFT JOIN  DB_T_PROD_STAG.cctl_reasonforuse ON cctl_reasonforuse.ID_stg = cc_incident.VehicleUseReason_stg 

join (select cc_claim.id_stg, cc_claim.State_stg, cc_claim.updatetime_stg, cc_claim.Retired_stg, cc_claim.LossDate_stg from DB_T_PROD_STAG.cc_Claim inner join DB_T_PROD_STAG.cctl_claimstate on cc_claim.State_stg= cctl_claimstate.id_stg where cctl_claimstate.name_stg <> ''Draft'') cc_Claim on cc_claim.id_stg=cc_incident.ClaimID_stg

 JOIN  DB_T_PROD_STAG.cc_claimcontactrole ON cc_claimcontactrole.claimcontactid_stg =cc_incident.id_stg

 JOIN  DB_T_PROD_STAG.cctl_contactrole ON cctl_contactrole.id_stg=cc_claimcontactrole.role_stg

 JOIN  DB_T_PROD_STAG.cc_claimcontact ON cc_claimcontact.id_stg=cc_claimcontactrole.claimcontactid_stg

 JOIN  DB_T_PROD_STAG.cc_Contact ON cc_Contact.id_stg=cc_claimcontact.contactid_stg

 JOIN  DB_T_PROD_STAG.cctl_vehicletype ON cctl_vehicletype.id_stg=cc_incident.vehicletype_stg

 left join DB_T_PROD_STAG.cctl_contact on cctl_contact.id_stg=cc_contact.subtype_stg

WHERE 

cctl_contactrole.typecode_stg=''insured''

AND (

(cc_incident.updatetime_stg > (:START_DTTM) and cc_incident.updatetime_stg <= (:END_DTTM))

OR (cc_vehicle.updatetime_stg > (:START_DTTM) and cc_vehicle.updatetime_stg <= (:END_DTTM))

OR (cc_claim.updatetime_stg > (:START_DTTM) and cc_claim.updatetime_stg <= (:END_DTTM))

OR (cc_claimcontactrole.updatetime_stg > (:START_DTTM) and cc_claimcontactrole.updatetime_stg <= (:END_DTTM))

OR (cc_claimcontact.updatetime_stg > (:START_DTTM) and cc_claimcontact.updatetime_stg <= (:END_DTTM))

OR (cc_Contact.updatetime_stg > (:START_DTTM) and cc_Contact.updatetime_stg <= (:END_DTTM))

))C

)CC_PRTY_TO_PRTY_ASSET

where  cc_prty_to_prty_asset.addressbookuid is not null  and role_cd not in (''MORTGAGEE'',''ASSET_ROLE_TYPE12'')

qualify ROW_NUMBER() OVER  (partition by PolicySystemId,sbtype,classification_code,src_cd,typecode,role_cd,addressbookuid order by UPDATETIME desc )=1

) SRC
)
);


-- Component exp_all_source, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_all_source AS
(
SELECT
SQ_cc_prty_to_prty_asset.PolicySystemId as PolicySystemId,
LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE */ as var_sbtype,
LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN */ as var_classification_code,
SQ_cc_prty_to_prty_asset.typecode as typecode,
UPPER ( SQ_cc_prty_to_prty_asset.typecode ) as var_typecode,
CASE WHEN SQ_cc_prty_to_prty_asset.role_cd = ''OWNER'' THEN ''ASSET_ROLE_TYPE12'' ELSE lower ( SQ_cc_prty_to_prty_asset.role_cd ) END as role_cd_var,
SQ_cc_prty_to_prty_asset.UPDATETIME as UPDATETIME,
LKP_3.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SRC_CD */ as v_src_cd,
DECODE ( TRUE , SQ_cc_prty_to_prty_asset.role_dt IS NULL , TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) , SQ_cc_prty_to_prty_asset.role_dt ) as var_ASSET_ROLE_DT,
var_ASSET_ROLE_DT as out_ASSET_ROLE_DT,
DECODE ( TRUE , LKP_4.PRTY_ASSET_ID /* replaced lookup LKP_PRTY_ASSET_ID */ IS NULL , 9999 , LKP_5.PRTY_ASSET_ID /* replaced lookup LKP_PRTY_ASSET_ID */ ) as var_PRTY_ASSET_ID,
var_PRTY_ASSET_ID as out_PRTY_ASSET_ID,
CASE WHEN v_src_cd != ''GWPC'' THEN ( DECODE ( TRUE , LKP_6.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_ROLE_CD */ IS NULL , ''UNK'' , LKP_7.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_ROLE_CD */ ) ) ELSE ( DECODE ( TRUE , LKP_8.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_ROLE_CD */ IS NULL , ''UNK'' , LKP_9.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_ROLE_CD */ ) ) END as var_ASSET_ROLE_CD,
var_ASSET_ROLE_CD as out_ASSET_ROLE_CD,
LKP_10.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_BUSN_CTGY */ as var_BUSN_CTGY_CD,
LKP_11.BUSN_PRTY_ID /* replaced lookup LKP_BUSN */ as var_BUSN_ID,
DECODE ( SQ_cc_prty_to_prty_asset.src_cd , ''GWPC'' , LKP_12.INDIV_PRTY_ID /* replaced lookup LKP_INDIV_CNT_MGR */ , ''GWCC'' , LKP_13.INDIV_PRTY_ID /* replaced lookup LKP_INDIV_CLM_CTR */ ) as var_INDIV_ID,
CASE WHEN var_BUSN_ID IS NULL THEN var_INDIV_ID ELSE var_BUSN_ID END as var_PRTY_ID,
CASE WHEN var_PRTY_ID IS NULL THEN 9999 ELSE var_PRTY_ID END as out_PRTY_ID,
DECODE ( TRUE , LKP_14.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_USE_CD */ IS NULL , ''UNK'' , LKP_15.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_USE_CD */ ) as var_ASSET_USE_CD,
var_ASSET_USE_CD as out_ASSET_USE_CD,
LKP_16.PRTY_ASSET_ID /* replaced lookup LKP_PRTY_TO_PRTY_ASSET */ as lkp_PRTY_ASSET_ID,
CASE WHEN SQ_cc_prty_to_prty_asset.RateDriverclassalfa IS NULL or ltrim ( rtrim ( SQ_cc_prty_to_prty_asset.RateDriverclassalfa ) ) = '''' THEN lpad ( '' '' , 50 , '' '' ) ELSE ltrim ( rtrim ( SQ_cc_prty_to_prty_asset.RateDriverclassalfa ) ) END as RateDriverclassalfa1,
LKP_17.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_LIENHOLDER_CD */ as in_lienholder_cd,
SQ_cc_prty_to_prty_asset.Retired as Retired,
--SQ_cc_prty_to_prty_asset.Rnk as Rnk,
SQ_cc_prty_to_prty_asset.source_record_id,
row_number() over (partition by SQ_cc_prty_to_prty_asset.source_record_id order by SQ_cc_prty_to_prty_asset.source_record_id) as RNK
FROM
SQ_cc_prty_to_prty_asset
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = SQ_cc_prty_to_prty_asset.sbtype
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = SQ_cc_prty_to_prty_asset.classification_code
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_SRC_CD LKP_3 ON LKP_3.SRC_IDNTFTN_VAL = SQ_cc_prty_to_prty_asset.src_cd
LEFT JOIN LKP_PRTY_ASSET_ID LKP_4 ON LKP_4.ASSET_HOST_ID_VAL = SQ_cc_prty_to_prty_asset.PolicySystemId AND LKP_4.PRTY_ASSET_SBTYPE_CD = var_sbtype AND LKP_4.PRTY_ASSET_CLASFCN_CD = var_classification_code
LEFT JOIN LKP_PRTY_ASSET_ID LKP_5 ON LKP_5.ASSET_HOST_ID_VAL = SQ_cc_prty_to_prty_asset.PolicySystemId AND LKP_5.PRTY_ASSET_SBTYPE_CD = var_sbtype AND LKP_5.PRTY_ASSET_CLASFCN_CD = var_classification_code
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_ROLE_CD LKP_6 ON LKP_6.SRC_IDNTFTN_VAL = SQ_cc_prty_to_prty_asset.typecode
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_ROLE_CD LKP_7 ON LKP_7.SRC_IDNTFTN_VAL = SQ_cc_prty_to_prty_asset.typecode
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_ROLE_CD LKP_8 ON LKP_8.SRC_IDNTFTN_VAL = role_cd_var
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_ROLE_CD LKP_9 ON LKP_9.SRC_IDNTFTN_VAL = role_cd_var
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_BUSN_CTGY LKP_10 ON LKP_10.SRC_IDNTFTN_VAL = SQ_cc_prty_to_prty_asset.typecode1
LEFT JOIN LKP_BUSN LKP_11 ON LKP_11.BUSN_CTGY_CD = var_BUSN_CTGY_CD AND LKP_11.NK_BUSN_CD = SQ_cc_prty_to_prty_asset.addressbookuid
LEFT JOIN LKP_INDIV_CNT_MGR LKP_12 ON LKP_12.NK_LINK_ID = SQ_cc_prty_to_prty_asset.addressbookuid
LEFT JOIN LKP_INDIV_CLM_CTR LKP_13 ON LKP_13.NK_PUBLC_ID = SQ_cc_prty_to_prty_asset.addressbookuid
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_USE_CD LKP_14 ON LKP_14.SRC_IDNTFTN_VAL = SQ_cc_prty_to_prty_asset.typecode
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_USE_CD LKP_15 ON LKP_15.SRC_IDNTFTN_VAL = SQ_cc_prty_to_prty_asset.typecode
LEFT JOIN LKP_PRTY_TO_PRTY_ASSET LKP_16 ON LKP_16.PRTY_ASSET_ID = var_PRTY_ASSET_ID AND LKP_16.ASSET_ROLE_CD = var_ASSET_ROLE_CD AND LKP_16.ASSET_ROLE_DTTM = var_ASSET_ROLE_DT AND LKP_16.PRTY_ID = var_PRTY_ID
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_LIENHOLDER_CD LKP_17 ON LKP_17.SRC_IDNTFTN_VAL = SQ_cc_prty_to_prty_asset.mortgageetype_alfa
QUALIFY RNK = 1
);


-- Component exp_SrcFields, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_SrcFields AS
(
SELECT
exp_all_source.out_PRTY_ASSET_ID as in_PRTY_ASSET_ID,
exp_all_source.out_ASSET_ROLE_CD as in_ASSET_ROLE_CD,
exp_all_source.out_ASSET_ROLE_DT as in_ASSET_ROLE_DT,
exp_all_source.out_PRTY_ID as in_PRTY_ID,
exp_all_source.out_ASSET_USE_CD as in_ASSET_USE_CD,
:PRCS_ID as in_PRCS_ID,
exp_all_source.RateDriverclassalfa1 as RateDriverclassalfa,
exp_all_source.UPDATETIME as UPDATETIME,
TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as EDW_END_DTTm,
exp_all_source.in_lienholder_cd as in_lienholder_cd,
exp_all_source.Retired as Retired,
exp_all_source.Rnk as Rnk,
exp_all_source.source_record_id
FROM
exp_all_source
);


-- Component LKP_PRTY_TO_PRTY_ASSET_CDC, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_PRTY_TO_PRTY_ASSET_CDC AS
(
SELECT
LKP.PRTY_ASSET_ID,
LKP.ASSET_ROLE_CD,
LKP.ASSET_ROLE_DTTM,
LKP.PRTY_ID,
LKP.ASSET_USE_CD,
LKP.DRVR_CLAS_CD,
LKP.LIENHLDR_POSITN_CD,
LKP.EDW_STRT_DTTM,
LKP.EDW_END_DTTM,
exp_SrcFields.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_SrcFields.source_record_id ORDER BY LKP.PRTY_ASSET_ID asc,LKP.ASSET_ROLE_CD asc,LKP.ASSET_ROLE_DTTM asc,LKP.PRTY_ID asc,LKP.ASSET_USE_CD asc,LKP.DRVR_CLAS_CD asc,LKP.LIENHLDR_POSITN_CD asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc) RNK
FROM
exp_SrcFields
LEFT JOIN (
SELECT	PRTY_TO_PRTY_ASSET.ASSET_ROLE_DTTM as ASSET_ROLE_DTTM, PRTY_TO_PRTY_ASSET.ASSET_USE_CD as ASSET_USE_CD,
		PRTY_TO_PRTY_ASSET.DRVR_CLAS_CD as DRVR_CLAS_CD, PRTY_TO_PRTY_ASSET.LIENHLDR_POSITN_CD as LIENHLDR_POSITN_CD,
		PRTY_TO_PRTY_ASSET.EDW_STRT_DTTM as EDW_STRT_DTTM,
		PRTY_TO_PRTY_ASSET.EDW_END_DTTM as EDW_END_DTTM, PRTY_TO_PRTY_ASSET.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
		PRTY_TO_PRTY_ASSET.PRTY_ASSET_ID as PRTY_ASSET_ID, PRTY_TO_PRTY_ASSET.ASSET_ROLE_CD as ASSET_ROLE_CD,
		PRTY_TO_PRTY_ASSET.PRTY_ID as PRTY_ID 
FROM	db_t_prod_core.PRTY_TO_PRTY_ASSET 
where	Asset_Role_Cd not in (''MRTGEE'',''OWNER'')
QUALIFY	ROW_NUMBER() OVER(
PARTITION BY PRTY_TO_PRTY_ASSET.PRTY_ASSET_ID,PRTY_TO_PRTY_ASSET.ASSET_ROLE_CD,
		PRTY_TO_PRTY_ASSET.PRTY_ID 
ORDER BY PRTY_TO_PRTY_ASSET.EDW_END_DTTM desc) = 1
) LKP ON LKP.PRTY_ASSET_ID = exp_SrcFields.in_PRTY_ASSET_ID AND LKP.ASSET_ROLE_CD = exp_SrcFields.in_ASSET_ROLE_CD AND LKP.PRTY_ID = exp_SrcFields.in_PRTY_ID
QUALIFY RNK = 1
);


-- Component exp_CDC_Check, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_CDC_Check AS
(
SELECT
exp_SrcFields.in_PRTY_ASSET_ID as in_PRTY_ASSET_ID,
exp_SrcFields.in_ASSET_ROLE_CD as in_ASSET_ROLE_CD,
exp_SrcFields.in_ASSET_ROLE_DT as in_ASSET_ROLE_DT,
exp_SrcFields.in_PRTY_ID as in_PRTY_ID,
exp_SrcFields.in_ASSET_USE_CD as in_ASSET_USE_CD,
exp_SrcFields.in_PRCS_ID as in_PRCS_ID,
LKP_PRTY_TO_PRTY_ASSET_CDC.PRTY_ASSET_ID as lkp_PRTY_ASSET_ID,
LKP_PRTY_TO_PRTY_ASSET_CDC.ASSET_ROLE_CD as lkp_ASSET_ROLE_CD,
LKP_PRTY_TO_PRTY_ASSET_CDC.ASSET_ROLE_DTTM as lkp_ASSET_ROLE_DT,
LKP_PRTY_TO_PRTY_ASSET_CDC.PRTY_ID as lkp_PRTY_ID,
LKP_PRTY_TO_PRTY_ASSET_CDC.ASSET_USE_CD as lkp_ASSET_USE_CD,
LKP_PRTY_TO_PRTY_ASSET_CDC.EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
LKP_PRTY_TO_PRTY_ASSET_CDC.EDW_END_DTTM as lkp_EDW_END_DTTM,
LKP_PRTY_TO_PRTY_ASSET_CDC.DRVR_CLAS_CD as lkp_DRVR_CLAS_CD,
exp_SrcFields.UPDATETIME as UPDATETIME,
exp_SrcFields.RateDriverclassalfa as RateDriverclassalfa,
MD5 ( ltrim ( rtrim ( exp_SrcFields.in_ASSET_ROLE_DT ) ) || ltrim ( rtrim ( exp_SrcFields.in_ASSET_USE_CD ) ) || ltrim ( rtrim ( exp_SrcFields.RateDriverclassalfa ) ) || ltrim ( rtrim ( exp_SrcFields.in_lienholder_cd ) ) ) as v_SRC_MD5,
MD5 ( ltrim ( rtrim ( LKP_PRTY_TO_PRTY_ASSET_CDC.ASSET_ROLE_DTTM ) ) || ltrim ( rtrim ( LKP_PRTY_TO_PRTY_ASSET_CDC.ASSET_USE_CD ) ) || ltrim ( rtrim ( LKP_PRTY_TO_PRTY_ASSET_CDC.DRVR_CLAS_CD ) ) || ltrim ( rtrim ( LKP_PRTY_TO_PRTY_ASSET_CDC.LIENHLDR_POSITN_CD ) ) ) as v_TGT_MD5,
CASE WHEN v_TGT_MD5 IS NULL THEN ''I'' ELSE CASE WHEN v_SRC_MD5 = v_TGT_MD5 THEN ''R'' ELSE ''U'' END END as o_SRC_TGT,
exp_SrcFields.EDW_END_DTTm as EndDate,
exp_SrcFields.in_lienholder_cd as in_lienholder_cd,
exp_SrcFields.Retired as Retired,
exp_SrcFields.Rnk as Rnk,
exp_SrcFields.source_record_id
FROM
exp_SrcFields
INNER JOIN LKP_PRTY_TO_PRTY_ASSET_CDC ON exp_SrcFields.source_record_id = LKP_PRTY_TO_PRTY_ASSET_CDC.source_record_id
);


-- Component rtr_CDC_Insert, Type ROUTER Output Group Insert
CREATE OR REPLACE TEMPORARY TABLE rtr_CDC_Insert AS (
SELECT
exp_CDC_Check.in_PRTY_ASSET_ID as in_PRTY_ASSET_ID,
exp_CDC_Check.in_ASSET_ROLE_CD as in_ASSET_ROLE_CD,
exp_CDC_Check.in_ASSET_ROLE_DT as in_ASSET_ROLE_DT,
exp_CDC_Check.in_PRTY_ID as in_PRTY_ID,
exp_CDC_Check.in_ASSET_USE_CD as in_ASSET_USE_CD,
exp_CDC_Check.in_PRCS_ID as in_PRCS_ID,
exp_CDC_Check.lkp_PRTY_ASSET_ID as lkp_PRTY_ASSET_ID,
exp_CDC_Check.lkp_ASSET_ROLE_CD as lkp_ASSET_ROLE_CD,
exp_CDC_Check.lkp_PRTY_ID as lkp_PRTY_ID,
exp_CDC_Check.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_CDC_Check.o_SRC_TGT as o_SRC_TGT,
exp_CDC_Check.EndDate as EndDate,
exp_CDC_Check.RateDriverclassalfa as RateDriverclassalfa,
exp_CDC_Check.UPDATETIME as TRANS_STRT_DTTM,
exp_CDC_Check.lkp_ASSET_ROLE_DT as lkp_ASSET_ROLE_DT,
exp_CDC_Check.lkp_ASSET_USE_CD as lkp_ASSET_USE_CD,
exp_CDC_Check.lkp_DRVR_CLAS_CD as lkp_DRVR_CLAS_CD,
exp_CDC_Check.in_lienholder_cd as in_lienholder_cd,
exp_CDC_Check.Retired as Retired,
exp_CDC_Check.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_CDC_Check.Rnk as Rnk,
exp_CDC_Check.source_record_id
FROM
exp_CDC_Check
WHERE exp_CDC_Check.o_SRC_TGT = ''I'' and exp_CDC_Check.in_PRTY_ASSET_ID <> 9999 and exp_CDC_Check.in_PRTY_ID <> 9999 OR ( exp_CDC_Check.Retired = 0 AND exp_CDC_Check.lkp_EDW_END_DTTM != TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ) OR ( exp_CDC_Check.o_SRC_TGT = ''U'' and exp_CDC_Check.in_PRTY_ASSET_ID != 9999 and exp_CDC_Check.in_PRTY_ID != 9999 AND exp_CDC_Check.lkp_EDW_END_DTTM = TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ) -- and exp_CDC_Check.in_PRTY_ID != 9999
);


-- Component rtr_CDC_Retired, Type ROUTER Output Group Retired
CREATE OR REPLACE TEMPORARY TABLE rtr_CDC_Retired AS (
SELECT
exp_CDC_Check.in_PRTY_ASSET_ID as in_PRTY_ASSET_ID,
exp_CDC_Check.in_ASSET_ROLE_CD as in_ASSET_ROLE_CD,
exp_CDC_Check.in_ASSET_ROLE_DT as in_ASSET_ROLE_DT,
exp_CDC_Check.in_PRTY_ID as in_PRTY_ID,
exp_CDC_Check.in_ASSET_USE_CD as in_ASSET_USE_CD,
exp_CDC_Check.in_PRCS_ID as in_PRCS_ID,
exp_CDC_Check.lkp_PRTY_ASSET_ID as lkp_PRTY_ASSET_ID,
exp_CDC_Check.lkp_ASSET_ROLE_CD as lkp_ASSET_ROLE_CD,
exp_CDC_Check.lkp_PRTY_ID as lkp_PRTY_ID,
exp_CDC_Check.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_CDC_Check.o_SRC_TGT as o_SRC_TGT,
exp_CDC_Check.EndDate as EndDate,
exp_CDC_Check.RateDriverclassalfa as RateDriverclassalfa,
exp_CDC_Check.UPDATETIME as TRANS_STRT_DTTM,
exp_CDC_Check.lkp_ASSET_ROLE_DT as lkp_ASSET_ROLE_DT,
exp_CDC_Check.lkp_ASSET_USE_CD as lkp_ASSET_USE_CD,
exp_CDC_Check.lkp_DRVR_CLAS_CD as lkp_DRVR_CLAS_CD,
exp_CDC_Check.in_lienholder_cd as in_lienholder_cd,
exp_CDC_Check.Retired as Retired,
exp_CDC_Check.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_CDC_Check.Rnk as Rnk,
exp_CDC_Check.source_record_id
FROM
exp_CDC_Check
WHERE exp_CDC_Check.o_SRC_TGT = ''R'' and exp_CDC_Check.Retired != 0 and exp_CDC_Check.lkp_EDW_END_DTTM = TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
);


-- Component upd_INSERT, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_INSERT AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_CDC_Insert.in_PRTY_ASSET_ID as in_PRTY_ASSET_ID1,
rtr_CDC_Insert.in_ASSET_ROLE_CD as in_ASSET_ROLE_CD1,
rtr_CDC_Insert.in_ASSET_ROLE_DT as in_ASSET_ROLE_DT1,
rtr_CDC_Insert.in_PRTY_ID as in_PRTY_ID1,
rtr_CDC_Insert.in_ASSET_USE_CD as in_ASSET_USE_CD1,
rtr_CDC_Insert.in_PRCS_ID as in_PRCS_ID1,
rtr_CDC_Insert.EndDate as EndDate1,
rtr_CDC_Insert.RateDriverclassalfa as RateDriverclassalfa1,
rtr_CDC_Insert.TRANS_STRT_DTTM as TRANS_STRT_DTTM1,
rtr_CDC_Insert.in_lienholder_cd as in_lienholder_cd1,
rtr_CDC_Insert.Retired as Retired1,
rtr_CDC_Insert.Rnk as Rnk1,
rtr_CDC_Insert.source_record_id,
0 as UPDATE_STRATEGY_ACTION
FROM
rtr_CDC_Insert
);


-- Component upd_retired, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_retired AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_CDC_Retired.lkp_PRTY_ASSET_ID as lkp_PRTY_ASSET_ID4,
rtr_CDC_Retired.lkp_PRTY_ID as lkp_PRTY_ID4,
rtr_CDC_Retired.lkp_ASSET_ROLE_CD as lkp_ASSET_ROLE_CD4,
rtr_CDC_Retired.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM4,
rtr_CDC_Retired.lkp_ASSET_ROLE_DT as lkp_ASSET_ROLE_DT4,
rtr_CDC_Retired.TRANS_STRT_DTTM as TRANS_STRT_DTTM4,
rtr_CDC_Retired.source_record_id,
1 as UPDATE_STRATEGY_ACTION
FROM
rtr_CDC_Retired
);


-- Component exp_RETIRED, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_RETIRED AS
(
SELECT
upd_retired.lkp_PRTY_ASSET_ID4 as lkp_PRTY_ASSET_ID4,
upd_retired.lkp_PRTY_ID4 as lkp_PRTY_ID4,
upd_retired.lkp_ASSET_ROLE_CD4 as lkp_ASSET_ROLE_CD4,
upd_retired.lkp_EDW_STRT_DTTM4 as lkp_EDW_STRT_DTTM4,
upd_retired.lkp_ASSET_ROLE_DT4 as lkp_ASSET_ROLE_DT4,
upd_retired.TRANS_STRT_DTTM4 as TRANS_STRT_DTTM41,
CURRENT_TIMESTAMP as o_EDW_END_DTTM,
upd_retired.source_record_id
FROM
upd_retired
);


-- Component exp_INSERT, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_INSERT AS
(
SELECT
upd_INSERT.in_PRTY_ASSET_ID1 as in_PRTY_ASSET_ID1,
upd_INSERT.in_ASSET_ROLE_CD1 as in_ASSET_ROLE_CD1,
upd_INSERT.in_ASSET_ROLE_DT1 as in_ASSET_ROLE_DT1,
upd_INSERT.in_PRTY_ID1 as in_PRTY_ID1,
upd_INSERT.in_ASSET_USE_CD1 as in_ASSET_USE_CD1,
upd_INSERT.in_PRCS_ID1 as in_PRCS_ID1,
upd_INSERT.RateDriverclassalfa1 as RateDriverclassalfa1,
upd_INSERT.TRANS_STRT_DTTM1 as TRANS_STRT_DTTM1,
upd_INSERT.in_lienholder_cd1 as in_lienholder_cd1,
CASE WHEN upd_INSERT.Retired1 != 0 THEN CURRENT_TIMESTAMP ELSE upd_INSERT.EndDate1 END as o_EDW_END_DTTM,
CASE WHEN upd_INSERT.Retired1 != 0 THEN upd_INSERT.TRANS_STRT_DTTM1 ELSE to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) END as TRANS_END_DTTM,
CASE WHEN upd_INSERT.Retired1 = 0 THEN dateadd (second, ( 2 * ( upd_INSERT.Rnk1 - 1 ) ) , CURRENT_TIMESTAMP  ) ELSE CURRENT_TIMESTAMP END as StartDate1,
upd_INSERT.source_record_id
FROM
upd_INSERT
);


-- Component PRTY_TO_PRTY_ASSET_Retired, Type TARGET 
MERGE INTO DB_T_PROD_CORE.PRTY_TO_PRTY_ASSET
USING exp_RETIRED ON (PRTY_TO_PRTY_ASSET.PRTY_ASSET_ID = exp_RETIRED.lkp_PRTY_ASSET_ID4 AND PRTY_TO_PRTY_ASSET.ASSET_ROLE_CD = exp_RETIRED.lkp_ASSET_ROLE_CD4 AND PRTY_TO_PRTY_ASSET.PRTY_ID = exp_RETIRED.lkp_PRTY_ID4 AND PRTY_TO_PRTY_ASSET.EDW_STRT_DTTM = exp_RETIRED.lkp_EDW_STRT_DTTM4)
WHEN MATCHED THEN UPDATE
SET
PRTY_ASSET_ID = exp_RETIRED.lkp_PRTY_ASSET_ID4,
ASSET_ROLE_CD = exp_RETIRED.lkp_ASSET_ROLE_CD4,
ASSET_ROLE_DTTM = exp_RETIRED.lkp_ASSET_ROLE_DT4,
PRTY_ID = exp_RETIRED.lkp_PRTY_ID4,
EDW_STRT_DTTM = exp_RETIRED.lkp_EDW_STRT_DTTM4,
EDW_END_DTTM = exp_RETIRED.o_EDW_END_DTTM,
TRANS_END_DTTM = exp_RETIRED.TRANS_STRT_DTTM41;


-- Component PRTY_TO_PRTY_ASSET_NewInsert, Type TARGET 
INSERT INTO DB_T_PROD_CORE.PRTY_TO_PRTY_ASSET
(
PRTY_ASSET_ID,
ASSET_ROLE_CD,
ASSET_ROLE_DTTM,
PRTY_ID,
ASSET_USE_CD,
LIENHLDR_POSITN_CD,
DRVR_CLAS_CD,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM,
TRANS_END_DTTM
)
SELECT
exp_INSERT.in_PRTY_ASSET_ID1 as PRTY_ASSET_ID,
exp_INSERT.in_ASSET_ROLE_CD1 as ASSET_ROLE_CD,
exp_INSERT.in_ASSET_ROLE_DT1 as ASSET_ROLE_DTTM,
exp_INSERT.in_PRTY_ID1 as PRTY_ID,
exp_INSERT.in_ASSET_USE_CD1 as ASSET_USE_CD,
exp_INSERT.in_lienholder_cd1 as LIENHLDR_POSITN_CD,
exp_INSERT.RateDriverclassalfa1 as DRVR_CLAS_CD,
exp_INSERT.in_PRCS_ID1 as PRCS_ID,
exp_INSERT.StartDate1 as EDW_STRT_DTTM,
exp_INSERT.o_EDW_END_DTTM as EDW_END_DTTM,
exp_INSERT.TRANS_STRT_DTTM1 as TRANS_STRT_DTTM,
exp_INSERT.TRANS_END_DTTM as TRANS_END_DTTM
FROM
exp_INSERT;


-- PIPELINE END FOR 1
-- Component PRTY_TO_PRTY_ASSET_NewInsert, Type Post SQL 
UPDATE  db_t_prod_core.PRTY_TO_PRTY_ASSET   
set 

EDW_END_DTTM=A.lead1,

TRANS_END_DTTM=A.lead2
FROM

(SELECT  distinct PRTY_ASSET_ID,ASSET_ROLE_CD,PRTY_ID,EDW_STRT_DTTM,TRANS_STRT_DTTM,

max(EDW_STRT_DTTM) over (partition by PRTY_ASSET_ID,ASSET_ROLE_CD,PRTY_ID ORDER BY EDW_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND'' 

 as lead1,

max(TRANS_STRT_DTTM) over (partition by PRTY_ASSET_ID,ASSET_ROLE_CD,PRTY_ID ORDER BY EDW_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND'' 

 as lead2

FROM      db_t_prod_core.PRTY_TO_PRTY_ASSET where ASSET_ROLE_CD NOT IN ( ''MRTGEE'',''OWNER'')

group by PRTY_ASSET_ID,ASSET_ROLE_CD,PRTY_ID,EDW_STRT_DTTM,TRANS_STRT_DTTM

) a



where PRTY_TO_PRTY_ASSET.EDW_STRT_DTTM = A.EDW_STRT_DTTM

AND PRTY_TO_PRTY_ASSET.PRTY_ASSET_ID=A.PRTY_ASSET_ID

and PRTY_TO_PRTY_ASSET.ASSET_ROLE_CD = A.ASSET_ROLE_CD

and PRTY_TO_PRTY_ASSET.PRTY_ID = A.PRTY_ID

and PRTY_TO_PRTY_ASSET.ASSET_ROLE_CD NOT IN ( ''MRTGEE'',''OWNER'')

AND CAST(EDW_END_DTTM AS DATE)=''9999-12-31''

AND CAST(TRANS_END_DTTM AS DATE)=''9999-12-31''

and lead1 is not null;


-- PIPELINE START FOR 2

-- Component SQ_cc_prty_to_prty_asset_Owner, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_cc_prty_to_prty_asset_Owner AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as PolicySystemId,
$2 as sbtype,
$3 as classification_code,
$4 as src_cd,
$5 as typecode,
$6 as role_cd,
$7 as role_dt,
$8 as addressbookuid,
$9 as CTL_ID,
$10 as LOAD_USER,
$11 as RateDriverclassalfa,
$12 as creationdate,
$13 as expirationdate,
$14 as UPDATETIME,
$15 as mortgageetype_alfa,
$16 as Retired,
$17 as typecode1,
$18 as Rnk,
$19 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT x2.PolicySystemId,x2.sbtype,

 x2.classification_code,x2.src_cd,

 x2.typecode,x2.role_cd,x2.role_dt,

 x2.addressbookuid,

 x2.CTL_ID,

 x2.LOAD_USER,

 x2.RateDriverclassalfa,

 x2.creationdate,

 x2.expirationdate,

 x2.UPDATETIME,

 x2.mortgageetype_alfa,

 x2.Retired,

 x2.typecd,

 x2.rnk from (

 SELECT x.PolicySystemId,x.sbtype,x.classification_code,x.src_cd,x.typecode,x.role_cd,x.role_dt,x.addressbookuid,x.CTL_ID,x.LOAD_USER,x.RateDriverclassalfa,

 x.creationdate,x.expirationdate,x.UPDATETIME,x.mortgageetype_alfa,x.Retired,x.typecd,

 Rank()  OVER(PARTITION BY PolicySystemId,sbtype,classification_code,src_cd,typecode,role_cd,addressbookuid ORDER BY UPDATETIME desc )  as rnk 

 FROM (SELECT DISTINCT cc_prty_to_prty_asset.PolicySystemId, cc_prty_to_prty_asset.sbtype, cc_prty_to_prty_asset.classification_code, 

cc_prty_to_prty_asset.src_cd, cc_prty_to_prty_asset.typecode, cc_prty_to_prty_asset.role_cd, cc_prty_to_prty_asset.role_dt, 

substr(cc_prty_to_prty_asset.addressbookuid,1,regexp_instr(cc_prty_to_prty_asset.addressbookuid,''-'',1)-1) as addressbookuid, 

cc_prty_to_prty_asset.CTL_ID, cc_prty_to_prty_asset.LOAD_USER, cc_prty_to_prty_asset.RateDriverclassalfa, 

cc_prty_to_prty_asset.creationdate, cc_prty_to_prty_asset.expirationdate, cc_prty_to_prty_asset.UPDATETIME, cc_prty_to_prty_asset.mortgageetype_alfa, cc_prty_to_prty_asset.Retired,

substr(cc_prty_to_prty_asset.addressbookuid,regexp_instr(cc_prty_to_prty_asset.addressbookuid,''-'',1)+1) as typecd

FROM

 (

SELECT * FROM(

SELECT cast(PolicySystemId as varchar(100))as PolicySystemId, sbtype, CLASSIFICATION_CODE, SRC_CD, role_cd, role_dt,TYPECODE, addressbookuid, updatetime, creationdate,

expirationdate,RateDriverclassalfa,mortgageetype_alfa,retired, CTL_ID,LOAD_USER  from 

(SELECT distinct

pc_personalvehicle.FixedID_stg as PolicySystemId,

 cast(''PRTY_ASSET_SBTYPE4'' as varchar(100))AS sbtype, 

 cast(''PRTY_ASSET_CLASFCN3'' as varchar(100))as  classification_code,

 cast(''GWPC'' as varchar(50))as src_cd, 

 cast(''ASSET_ROLE_TYPE12''as varchar(100)) AS role_cd,

 cast(case when  pc_personalvehicle.PurchaseDate_Alfa_stg is null then cast(''1900-01-01 00:00:00.000000'' AS TIMESTAMP(6)) else pc_personalvehicle.PurchaseDate_Alfa_stg end as timestamp(6)) AS role_dt,

 pctl_contact.TYPECODE_stg as typecode,

 coalesce(pc_contact.addressbookuid_stg,'''')||''-''||pctl_contact.TYPECODE_stg AS addressbookuid,

 cast(''1'' as SMALLINT) as CTL_ID,

 (:p_load_user) as LOAD_USER,

 cast(pc_personalvehicle.UpdateTime_stg as TIMESTAMP(6))as UPDATETIME,

 cast(pc_personalvehicle.CreateTime_stg as TIMESTAMP(6))as creationdate,

 cast(case when pc_personalvehicle.ExpirationDate_stg is null then cast(''1900-01-01'' as date  ) else 

 pc_personalvehicle.ExpirationDate_stg end as TIMESTAMP(6))as expirationdate,

pc_personalvehicle.RateDriverClass_alfa_stg as RateDriverclassalfa,

cast(null as varchar(5))as mortgageetype_alfa,

case when (pc_policyperiod.Retired_stg=0 and pc_contact.Retired_stg=0) then 0 else 1 end as Retired,

row_number () over(partition by pc_personalvehicle.fixedid_stg order by TERMNUMBER_stg DESC,ModelNumber_stg desc ) rw

FROM  DB_T_PROD_STAG.pc_personalvehicle 

JOIN  DB_T_PROD_STAG.pc_policyperiod ON pc_policyperiod.id_stg = pc_personalvehicle.branchid_stg 

JOIN DB_T_PROD_STAG.pc_policycontactrole ON pc_policyperiod.id_stg = pc_policycontactrole.branchid_stg 

JOIN DB_T_PROD_STAG.pc_contact ON pc_contact.ID_stg = pc_policycontactrole.ContactDenorm_stg 

JOIN DB_T_PROD_STAG.pctl_contact on pctl_contact.id_stg=pc_contact.Subtype_stg 

JOIN DB_T_PROD_STAG.pctl_policycontactrole ON pc_policycontactrole.Subtype_stg = pctl_policycontactrole.ID_stg AND pctl_policycontactrole.name_stg = ''PolicyPriNamedInsured''

where pc_personalvehicle.ExpirationDate_stg is null AND(

(pc_personalvehicle.updatetime_stg > (:START_DTTM) and pc_personalvehicle.updatetime_stg <= (:END_DTTM))

OR (pc_policyperiod.updatetime_stg > (:START_DTTM) and pc_policyperiod.updatetime_stg <= (:END_DTTM))

OR (pc_policycontactrole.updatetime_stg > (:START_DTTM) and pc_policycontactrole.updatetime_stg <= (:END_DTTM))

OR (pc_contact.updatetime_stg > (:START_DTTM) and pc_contact.updatetime_stg <= (:END_DTTM)))

)A WHERE RW=1 



UNION



/** Dwelling Purchased Date by Owner**/



SELECT distinct PolicySystemId, sbtype, classification_code, src_cd, role_cd, cast(case when role_dt is null then cast(''1900-01-01 00:00:00.000000'' AS TIMESTAMP(6)) else role_dt end as TIMESTAMP(6))role_dt,

typecode, addressbookuid, UpdateTime, creationdate, Expirationdate, RateDriverclassalfa, mortgageetype_alfa, retired, CTL_ID, LOAD_USER FROM (

SELECT distinct

cast(pcx_dwelling_hoe.FixedID_stg as varchar(100)) as PolicySystemId, 

cast(''PRTY_ASSET_SBTYPE5'' as varchar(100)) AS sbtype, 

cast(''PRTY_ASSET_CLASFCN1'' as varchar(100))as classification_code,

cast(''GWPC'' as varchar(50)) as src_cd, 

 cast(''ASSET_ROLE_TYPE12'' as varchar(100)) AS role_cd,

 cast(cast(cast(YearPurchased_stg as varchar(100))||''-''||trim(cast(

		case 

			when pcx_dwelling_hoe.MonthPurchased_alfa_stg in (''1'',''2'',''3'',

		''4'',''5'',''6'',''7'',''8'',''9'') then 0||trim(pcx_dwelling_hoe.MonthPurchased_alfa_stg) 

			else pcx_dwelling_hoe.MonthPurchased_alfa_stg 

		end as varchar(100))||''-''||''01'') as date)as  timestamp(6))role_dt,

pctl_contact.TYPECODE_stg as typecode,

coalesce(pc_contact.addressbookuid_stg,'''')||''-''||pctl_contact.TYPECODE_stg as  addressbookuid,

cast(pcx_dwelling_hoe.UpdateTime_stg as TIMESTAMP(6))as UpdateTime,

cast(''1900-01-01 00:00:00.000000'' as TIMESTAMP(6)) as creationdate,

cast(''1900-01-01 00:00:00.000000'' as TIMESTAMP(6)) as Expirationdate,

cast(NULL as varchar(5)) RateDriverclassalfa,

cast(null as varchar(5)) as mortgageetype_alfa,

case when (pc_policyperiod.Retired_stg=0 and pc_contact.Retired_stg=0) then 0 else 1 end as retired,

cast(''1'' as SMALLINT) as CTL_ID,

(:p_load_user) as LOAD_USER

FROM  DB_T_PROD_STAG.pcx_dwelling_hoe 

JOIN  DB_T_PROD_STAG.pc_policyperiod ON pc_policyperiod.id_stg = pcx_dwelling_hoe.branchid_stg 

JOIN DB_T_PROD_STAG.pc_policycontactrole ON pc_policyperiod.id_stg = pc_policycontactrole.branchid_stg 

JOIN DB_T_PROD_STAG.pc_contact ON pc_contact.ID_stg = pc_policycontactrole.ContactDenorm_stg 

JOIN DB_T_PROD_STAG.pctl_contact on pctl_contact.id_stg=pc_contact.Subtype_stg 

JOIN DB_T_PROD_STAG.pctl_policycontactrole ON pc_policycontactrole.Subtype_stg = pctl_policycontactrole.ID_stg AND pctl_policycontactrole.name_stg = ''PolicyPriNamedInsured'' 

where  pcx_dwelling_hoe.ExpirationDate_stg is null/*EIM 13678*/ 

AND (

 (pcx_dwelling_hoe.updatetime_stg > (:START_DTTM) and pcx_dwelling_hoe.updatetime_stg <= (:END_DTTM))

OR (pc_policyperiod.updatetime_stg > (:START_DTTM) and pc_policyperiod.updatetime_stg <= (:END_DTTM))

OR (pc_policycontactrole.updatetime_stg > (:START_DTTM) and pc_policycontactrole.updatetime_stg <= (:END_DTTM))

OR (pc_contact.updatetime_stg > (:START_DTTM) and pc_contact.updatetime_stg <= (:END_DTTM)))



/*EIM-49111 FARM CHANGES*/

UNION

SELECT distinct

cast(pcx_fopdwelling.FixedID_stg as varchar(100)) as PolicySystemId, 

cast(''PRTY_ASSET_SBTYPE37'' as varchar(100)) AS sbtype, 

cast(''PRTY_ASSET_CLASFCN15'' as varchar(100))as classification_code,

cast(''GWPC'' as varchar(50)) as src_cd, 

 cast(''ASSET_ROLE_TYPE12'' as varchar(100)) AS role_cd,

 cast(cast(cast(YearPurchased_stg as varchar(100))||''-''||trim(cast(

        case 

            when pcx_fopdwelling.MonthPurchased_stg in (''1'',''2'',''3'',

        ''4'',''5'',''6'',''7'',''8'',''9'') then 0||trim(pcx_fopdwelling.MonthPurchased_stg) 

            else pcx_fopdwelling.MonthPurchased_stg 

        end as varchar(100))||''-''||''01'') as date)as  timestamp(6))role_dt,

pctl_contact.TYPECODE_stg as typecode,

coalesce(pc_contact.addressbookuid_stg,'''')||''-''||pctl_contact.TYPECODE_stg as  addressbookuid,

pcx_fopdwelling.UpdateTime_stg as UpdateTime,

cast(''1900-01-01 00:00:00.000000'' as TIMESTAMP(6)) as creationdate,

cast(''1900-01-01 00:00:00.000000'' as TIMESTAMP(6)) as Expirationdate,

cast(NULL as varchar(50)) RateDriverclassalfa,

cast(null as varchar(50)) as mortgageetype_alfa,

case when (pc_policyperiod.Retired_stg=0 and pc_contact.Retired_stg=0) then 0 else 1 end as retired,

cast(''1'' as SMALLINT) as CTL_ID,

(:p_load_user) as LOAD_USER

FROM  DB_T_PROD_STAG.pcx_fopdwelling 

JOIN  DB_T_PROD_STAG.pc_policyperiod ON pc_policyperiod.id_stg = pcx_fopdwelling.branchid_stg 

JOIN DB_T_PROD_STAG.pc_policycontactrole ON pc_policyperiod.id_stg = pc_policycontactrole.branchid_stg 

JOIN DB_T_PROD_STAG.pc_contact ON pc_contact.ID_stg = pc_policycontactrole.ContactDenorm_stg 

JOIN DB_T_PROD_STAG.pctl_contact on pctl_contact.id_stg=pc_contact.Subtype_stg 

JOIN DB_T_PROD_STAG.pctl_policycontactrole ON pc_policycontactrole.Subtype_stg = pctl_policycontactrole.ID_stg AND pctl_policycontactrole.name_stg = ''PolicyPriNamedInsured'' 

where (  pcx_fopdwelling.ExpirationDate_stg is null or  pcx_fopdwelling.ExpirationDate_stg>EditeffectiveDate_stg)

AND (

 (pcx_fopdwelling.updatetime_stg > (:START_DTTM) and pcx_fopdwelling.updatetime_stg <= (:END_DTTM))

OR (pc_policyperiod.updatetime_stg > (:START_DTTM) and pc_policyperiod.updatetime_stg <= (:END_DTTM))

OR (pc_policycontactrole.updatetime_stg > (:START_DTTM) and pc_policycontactrole.updatetime_stg <= (:END_DTTM))

OR (pc_contact.updatetime_stg > (:START_DTTM) and pc_contact.updatetime_stg <= (:END_DTTM)))

Qualify row_number() over(partition by pcx_fopdwelling.FixedID_stg order by coalesce(pcx_fopdwelling.expirationdate_stg, cast(''9999-12-31 23:59:59.999999'' as timestamp(6)) ) desc,pcx_fopdwelling.createtime_stg desc,pcx_fopdwelling.updatetime_stg desc)=1

) X

/*EIM-49111*/



UNION



select distinct 

cast(pc_personalvehicle.FixedID_stg as varchar(100))as  PolicySystemId,

cast(''PRTY_ASSET_SBTYPE4'' as varchar(100)) as sbtype, 

cast(''PRTY_ASSET_CLASFCN3'' as varchar(100))as classification_code,

cast(''GWPC'' as varchar(50))as src_cd,

pctl_additionalinteresttype.TYPECODE_stg as  role_cd,

cast(case when a.UpdateTime_stg is null then cast(''1900-01-01 00:00:00.000000'' AS TIMESTAMP(6)) else a.UpdateTime_stg end as timestamp(6))as role_dt,

pctl_contact.TYPECODE_stg as  typecode,

g.AddressBookUID_stg||''-''||pctl_contact.TYPECODE_stg as  addressbookuid,

cast(a.updatetime_stg as TIMESTAMP(6))as UpdateTime,

cast(''1900-01-01 00:00:00.000000'' as TIMESTAMP(6)) as creationdate,

cast(''1900-01-01 00:00:00.000000'' as TIMESTAMP(6)) as Expirationdate,

pc_personalvehicle.RateDriverClass_alfa_stg as RateDriverclassalfa,

pctl_mortgageetype_alfa.TYPECODE_stg as mortgageetype_alfa,

case when (d.Retired_stg=0 and f.Retired_stg=0 and g.Retired_stg=0) then 0 else 1 end as retired,

cast(''1'' as SMALLINT) as CTL_ID,

(:p_load_user) as LOAD_USER

from DB_T_PROD_STAG.pc_addlinterestdetail a 

inner join DB_T_PROD_STAG.pc_personalvehicle on a.PAVehicle_stg=pc_personalvehicle.id_stg

inner join DB_T_PROD_STAG.pc_policycontactrole b on b.id_stg = a.PolicyAddlInterest_stg

inner join DB_T_PROD_STAG.pctl_policycontactrole c on c.id_stg = b.Subtype_stg

inner join DB_T_PROD_STAG.pc_accountcontactrole d on d.id_stg = b.AccountContactRole_stg

inner join DB_T_PROD_STAG.pctl_accountcontactrole e on e.id_stg = d.Subtype_stg

inner join DB_T_PROD_STAG.pc_accountcontact f on f.id_stg = d.AccountContact_stg

inner join DB_T_PROD_STAG.pc_contact g on g.id_stg = f.Contact_stg

inner join DB_T_PROD_STAG.pctl_additionalinteresttype on pctl_additionalinteresttype.id_stg=a.AdditionalInterestType_stg

inner join DB_T_PROD_STAG.pctl_contact on pctl_contact.id_stg=g.Subtype_stg

left outer join DB_T_PROD_STAG.pctl_mortgageetype_alfa on a.MortgageeType_alfa_stg=pctl_mortgageetype_alfa.ID_stg

where e.TYPECODE_stg = ''AdditionalInterest'' and pctl_additionalinteresttype.TYPECODE_stg not in (''LHFirst_alfa'') 

AND (

(a.updatetime_stg > (:START_DTTM) and a.updatetime_stg <= (:END_DTTM))

OR (pc_personalvehicle.updatetime_stg > (:START_DTTM) and pc_personalvehicle.updatetime_stg <= (:END_DTTM))

OR (b.updatetime_stg > (:START_DTTM) and b.updatetime_stg <= (:END_DTTM))

OR (d.updatetime_stg > (:START_DTTM) and d.updatetime_stg <= (:END_DTTM))

OR (f.updatetime_stg > (:START_DTTM) and f.updatetime_stg <= (:END_DTTM))

OR (g.updatetime_stg > (:START_DTTM) and g.updatetime_stg <= (:END_DTTM)))



UNION



/** Lienholder Info for Dwelling**/

select distinct 

cast(pcx_dwelling_hoe.FixedID_stg as varchar(100))as PolicySystemId ,

cast(''PRTY_ASSET_SBTYPE5'' as varchar(100)) AS sbtype,

cast(''PRTY_ASSET_CLASFCN1'' as varchar(100))as classification_code,

cast(''GWPC''as varchar(50)) as src_cd, 

pctl_additionalinteresttype.TYPECODE_stg as role_cd,

CAST(case when a.UpdateTime_stg is null then cast(''1900-01-01 00:00:00.000000'' AS TIMESTAMP(6)) else a.UpdateTime_stg end as TIMESTAMP(6))as role_dt,

pctl_contact.TYPECODE_stg as typecode,

g.AddressBookUID_stg||''-''||pctl_contact.TYPECODE_stg as addressbookuid,

cast(a.updatetime_stg as TIMESTAMP(6))as updatetime,

cast(''1900-01-01 00:00:00.000000'' as TIMESTAMP(6)) as creationdate,

cast(''1900-01-01 00:00:00.000000'' as TIMESTAMP(6)) as Expirationdate,

cast(NULL as varchar(5))as RateDriverclassalfa,

pctl_mortgageetype_alfa.TYPECODE_stg as mortgageetype_alfa,

case when (d.Retired_stg=0 and f.Retired_stg=0 and g.Retired_stg=0) then 0 else 1 end as retired,

cast(''1'' as SMALLINT) as CTL_ID,

(:p_load_user) as LOAD_USER

from DB_T_PROD_STAG.pc_addlinterestdetail a 

inner join DB_T_PROD_STAG.pcx_dwelling_hoe on a.Dwelling_stg=pcx_dwelling_hoe.id_stg

inner join DB_T_PROD_STAG.pc_policycontactrole b on b.id_stg = a.PolicyAddlInterest_stg

inner join DB_T_PROD_STAG.pctl_policycontactrole c on c.id_stg = b.Subtype_stg

inner join DB_T_PROD_STAG.pc_accountcontactrole d on d.id_stg = b.AccountContactRole_stg

inner join DB_T_PROD_STAG.pctl_accountcontactrole e on e.id_stg = d.Subtype_stg

inner join DB_T_PROD_STAG.pc_accountcontact f on f.id_stg = d.AccountContact_stg

inner join DB_T_PROD_STAG.pc_contact g on g.id_stg = f.Contact_stg

inner join DB_T_PROD_STAG.pctl_additionalinteresttype on pctl_additionalinteresttype.id_stg=a.AdditionalInterestType_stg

inner join DB_T_PROD_STAG.pctl_contact on pctl_contact.id_stg=g.Subtype_stg

left outer join DB_T_PROD_STAG.pctl_mortgageetype_alfa on a.MortgageeType_alfa_stg=pctl_mortgageetype_alfa.ID_stg

where e.TYPECODE_stg = ''AdditionalInterest''

AND (

(a.updatetime_stg > (:START_DTTM) and a.updatetime_stg <= (:END_DTTM))

OR (pcx_dwelling_hoe.updatetime_stg > (:START_DTTM) and pcx_dwelling_hoe.updatetime_stg <= (:END_DTTM))

OR (b.updatetime_stg > (:START_DTTM) and b.updatetime_stg <= (:END_DTTM))

OR (d.updatetime_stg > (:START_DTTM) and d.updatetime_stg <= (:END_DTTM))

OR (f.updatetime_stg > (:START_DTTM) and f.updatetime_stg <= (:END_DTTM))

OR (g.updatetime_stg > (:START_DTTM) and g.updatetime_stg <= (:END_DTTM))

))A

union all



SELECT * FROM (

SELECT DISTINCT cast(id as varchar(100))as PolicySystemId, sbtype, classification_code, src_cd, role_cd, CAST(case when roledate is null then cast(''1900-01-01 00:00:00.000000'' AS TIMESTAMP(6)) else roledate end as TIMESTAMP(6))as role_dt, TYPECODE, addressbookuid, updatetime, 

creationdate, expirationdate, RateDriverclassalfa, mortgageetype_alfa,retired, CTL_ID, LOAD_USER FROM( 

select distinct  id,sbtype,classification_code,src_cd,role_cd,

(case when (cast(lag_exp as date)=cast(''9999-12-31'' as date)) then coalesce(lag_eff,roledt,min(periodstart) over (partition by id,addressbookuid order by termnumber,modelnumber,updatetime)) else coalesce(roledt,lag_eff,periodstart) end) roledate,

TYPECODE,

addressbookuid,

cast(updatetime as TIMESTAMP(6)) as updatetime,

cast(creationdate as timestamp(6))as creationdate , 

cast(expirationdate as timestamp(6))as expirationdate,

RateDriverclassalfa,

mortgageetype_alfa,

retired,cast(''1'' as SMALLINT) as CTL_ID,

(:p_load_user) as LOAD_USER from

(

select distinct id,sbtype,addressbookuid,classification_code,src_cd,role_cd,roledt,updatetime,TYPECODE,expirationdate,periodstart,effectivedate,

lag(roledt) over(partition by id,addressbookuid order by termnumber,modelnumber,updatetime) lag_eff, lag_exp,

creationdate, RateDriverclassalfa,

mortgageetype_alfa,

retired,start_dttm,end_dttm,termnumber,modelnumber

from

(

select distinct

id,sbtype,classification_code,src_cd,role_cd,periodstart,effectivedate,

(case when effectivedate is null then (lag(effectivedate) over(partition by id,addressbookuid order by termnumber,modelnumber,updatetime)) else effectivedate end) roledt,

TYPECODE,addressbookuid,updatetime,creationdate,

expirationdate, lag(expirationdate) over (order by id,updatetime)lag_exp,

RateDriverclassalfa,

mortgageetype_alfa,

retired,start_dttm,end_dttm,termnumber,modelnumber,rnk

from

(

select distinct id,

cast(''PRTY_ASSET_SBTYPE4'' as varchar(100)) as sbtype ,

cast(''PRTY_ASSET_CLASFCN3'' as varchar(100)) as classification_code,

cast(''GWPC'' as varchar(50))as src_cd,

 termnumber, modelnumber, addressbookuid,TYPECODE,role_cd,mortgageetype_alfa,

(case when (effectivedate is null and rnk=1 ) then periodstart else effectivedate end) as effectivedate, 

(case when (expirationdate is null) then cast(''9999-12-31'' as date)  else expirationdate end) as expirationdate,updatetime, RateDriverclassalfa,

cast(''1900-01-01 00:00:00.000000'' as TIMESTAMP(6)) as creationdate,cast (''1900-01-01'' as date  ) as start_dttm,

cast (''9999-01-01'' as date  ) as end_dttm,

periodstart,periodend,retired,rnk from

(

select distinct  id, termnumber, modelnumber, updatetime, RateDriverclassalfa,

effectivedate, expirationdate,addressbookuid,TYPECODE,role_cd, mortgageetype_alfa,

  periodstart, periodend,retired,

rank() over(partition by id,addressbookuid order by termnumber,modelnumber,updatetime) rnk from

(

select distinct pc_personalvehicle.FixedID_stg as id, 

pol.termnumber_stg as termnumber,

pol.modelnumber_stg as  modelnumber, addl.updatetime_stg as  updatetime, pc_personalvehicle.RateDriverClass_alfa_stg as  RateDriverclassalfa,

addl.effectivedate_stg as  effectivedate, pctl_contact.TYPECODE_stg as TYPECODE,pctl_additionalinteresttype.TYPECODE_stg as role_cd,

addl.expirationdate_stg as expirationdate, g.AddressBookUID_stg||''-''||pctl_contact.TYPECODE_stg as addressbookuid, pctl_mortgageetype_alfa.TYPECODE_stg as mortgageetype_alfa, case when (d.Retired_stg=0 and f.Retired_stg=0 and g.Retired_stg=0) then 0 else 1 end as retired,

pol.periodStart_stg as periodstart,

pol.periodEnd_stg as periodend

from DB_T_PROD_STAG.pc_addlinterestdetail addl 

inner JOIN  DB_T_PROD_STAG.pc_policyperiod pol ON  pol.id_stg=addl.BranchID_stg

inner join DB_T_PROD_STAG.pc_personalvehicle on addl.PAVehicle_stg=pc_personalvehicle.id_stg

inner join DB_T_PROD_STAG.pc_policycontactrole b on b.id_stg = addl.PolicyAddlInterest_stg

inner join DB_T_PROD_STAG.pctl_policycontactrole c on c.id_stg = b.Subtype_stg

inner join DB_T_PROD_STAG.pc_accountcontactrole d on d.id_stg = b.AccountContactRole_stg

inner join DB_T_PROD_STAG.pctl_accountcontactrole e on e.id_Stg = d.Subtype_stg

inner join DB_T_PROD_STAG.pc_accountcontact f on f.id_stg = d.AccountContact_stg

inner join DB_T_PROD_STAG.pc_contact g on g.id_stg = f.Contact_stg

inner join DB_T_PROD_STAG.pctl_additionalinteresttype on pctl_additionalinteresttype.id_stg=addl.AdditionalInterestType_stg

inner join DB_T_PROD_STAG.pctl_contact on pctl_contact.id_stg=g.Subtype_stg

left outer join DB_T_PROD_STAG.pctl_mortgageetype_alfa on addl.MortgageeType_alfa_stg=pctl_mortgageetype_alfa.ID_stg

where

e.TYPECODE_stg = ''AdditionalInterest''  

and pctl_additionalinteresttype.TYPECODE_stg in (''LHFirst_alfa'') 

AND (

(addl.updatetime_stg > (:START_DTTM) and addl.updatetime_stg <= (:END_DTTM))

OR (pol.updatetime_stg > (:START_DTTM) and pol.updatetime_stg <= (:END_DTTM))

OR (pc_personalvehicle.updatetime_stg > (:START_DTTM) and pc_personalvehicle.updatetime_stg <= (:END_DTTM))

OR (b.updatetime_stg > (:START_DTTM) and b.updatetime_stg <= (:END_DTTM))

OR (d.updatetime_stg > (:START_DTTM) and d.updatetime_stg <= (:END_DTTM))

OR (f.updatetime_stg > (:START_DTTM) and f.updatetime_stg <= (:END_DTTM))

OR (g.updatetime_stg > (:START_DTTM) and g.updatetime_stg <= (:END_DTTM))

)

    ) innr_mst

       )rnk

   )lag_inr

   )lag_col

    )final)X)B

	

	union all

	

	SELECT * FROM(

select distinct 

case when cc_vehicle.PolicySystemId_stg is not null then SUBSTR(cc_vehicle.policysystemid_stg, POSITION('':''in cc_vehicle.policysystemid_stg)+1,LENGTH(cc_vehicle.policysystemid_stg))

when (cc_vehicle.PolicySystemId_stg is null and cc_vehicle.Vin_stg is not null) then  ''VIN:''||cc_vehicle.vin_stg 

when (cc_vehicle.PolicySystemId_stg is null and cc_vehicle.Vin_stg is null and cc_vehicle.LicensePlate_stg is not null) then ''LP:''||cc_vehicle.licenseplate_stg

when (cc_vehicle.PolicySystemId_stg is null and cc_vehicle.Vin_stg is null and cc_vehicle.LicensePlate_stg is null) then cc_vehicle.PublicID_stg

end as PolicySystemId,

cast(''PRTY_ASSET_SBTYPE4'' as varchar(100)) as sbtype ,

cast(''PRTY_ASSET_CLASFCN3'' as varchar(100)) as classification_code,

case when PolicySystemId_stg is null then cast(''GWCC'' as varchar(100))else cast(''GWPC'' as varchar(100)) end as src_cd,

cast(''ASSET_ROLE_TYPE9'' as varchar(100)) as role_cd,

cast(case when cc_incident.DateSalvageAssigned_stg is null then cast(''1900-01-01 00:00:00.000000'' AS TIMESTAMP(6)) else cc_incident.DateSalvageAssigned_stg end  as TIMESTAMP(6))as role_dt,

cast(''BUSN_CTGY5'' as varchar(100))as typecode,

cctl_salvageyard_alfa.TYPECODE_stg||''-''||cast(''BUSN_CTGY5'' as varchar(100))as addressbookuid,

CAST(cc_incident.updatetime_stg as TIMESTAMP(6))as UPDATETIME,

cast(''1900-01-01 00:00:00.000000'' as TIMESTAMP(6)) as creationdate,

cast(''1900-01-01 00:00:00.000000'' as TIMESTAMP(6)) as Expirationdate,

cast(NULL as varchar(5)) as  RateDriverclassalfa,

cast(null as varchar(50)) as mortgageetype_alfa,

case when (cc_incident.Retired_stg=0 and cc_vehicle.Retired_stg=0 ) 

then 0 else 1 end  as retired,

cast(''1'' as SMALLINT) as CTL_ID,

(:p_load_user) as LOAD_USER

from DB_T_PROD_STAG.cc_incident 

inner join DB_T_PROD_STAG.cc_vehicle on cc_incident.VehicleID_stg = cc_vehicle.ID_stg

left outer join DB_T_PROD_STAG.cctl_incident on cc_incident.Subtype_stg = cctl_incident.id_stg

left join DB_T_PROD_STAG.cctl_salvageyard_alfa on cctl_salvageyard_alfa.id_stg=cc_incident.SalvageYard_alfa_stg

where

(cc_incident.updatetime_stg > (:START_DTTM) and cc_incident.updatetime_stg <= (:END_DTTM))

OR (cc_vehicle.updatetime_stg > (:START_DTTM) and cc_vehicle.updatetime_stg <= (:END_DTTM))





Union



select distinct 

/** Party Asset **/

case when (cc_vehicle.PolicySystemId_stg is null and cc_vehicle.Vin_stg is not null) then  ''VIN:''||cc_vehicle.vin_stg 

when (cc_vehicle.PolicySystemId_stg is null and cc_vehicle.Vin_stg is null and cc_vehicle.LicensePlate_stg is not null) then ''LP:''||cc_vehicle.licenseplate_stg

when (cc_vehicle.PolicySystemId_stg is null and cc_vehicle.Vin_stg is null and cc_vehicle.LicensePlate_stg is null) then cc_vehicle.PublicID_stg

end as PolicySystemId,

cast(''PRTY_ASSET_SBTYPE4'' as varchar(100))as sbtype,

cast(''PRTY_ASSET_CLASFCN3'' as  varchar(100))as classification_code,

case when cc_vehicle.PolicySystemId_stg is null then cast(''GWCC''as varchar(100)) else cast(''GWPC'' as varchar(100)) end as src_cd,

cctl_vehicletype.typecode_stg as role_cd,

cast(case when cc_Claim.LossDate_stg is null then cast(''1900-01-01 00:00:00.000000'' AS TIMESTAMP(6)) else cc_Claim.LossDate_stg end as TIMESTAMP(6))as role_dt, 

cctl_reasonforuse.TYPECODE_stg  as TYPECODE,

cc_contact.PublicID_stg||''-''||cctl_contact.typecode_stg as addressbookuid, 

cast(cc_claimcontactrole.UpdateTime_stg as TIMESTAMP(6))as UpdateTime,

cast(''1900-01-01 00:00:00.000000'' as TIMESTAMP(6)) as creationdate,

cast(''1900-01-01 00:00:00.000000'' as TIMESTAMP(6)) as Expirationdate,

cast(NULL as varchar(50))as RateDriverclassalfa,

cast(null as varchar(50)) as mortgageetype_alfa,

case when (cc_incident.Retired_stg=0 and cc_vehicle.Retired_stg =0 

and cc_claim.Retired_stg=0 and cc_claimcontactrole.Retired_stg=0 and cc_claimcontact.Retired_stg=0 

and cc_contact.Retired_stg=0 ) 

then 0 else 1 end as retired,

cast(''1'' as SMALLINT) as CTL_ID,

(:p_load_user) as LOAD_USER

FROM DB_T_PROD_STAG.cc_incident 

inner join DB_T_PROD_STAG.cc_vehicle on cc_incident.VehicleID_stg = cc_vehicle.ID_stg

left outer join DB_T_PROD_STAG.cctl_incident on cc_incident.Subtype_stg = cctl_incident.id_stg

LEFT JOIN  DB_T_PROD_STAG.cctl_reasonforuse ON cctl_reasonforuse.ID_stg = cc_incident.VehicleUseReason_stg 

join (select cc_claim.id_stg, cc_claim.State_stg, cc_claim.updatetime_stg, cc_claim.Retired_stg, cc_claim.LossDate_stg from DB_T_PROD_STAG.cc_Claim inner join DB_T_PROD_STAG.cctl_claimstate on cc_claim.State_stg= cctl_claimstate.id_stg where cctl_claimstate.name_stg <> ''Draft'') cc_Claim on cc_claim.id_stg=cc_incident.ClaimID_stg

 JOIN  DB_T_PROD_STAG.cc_claimcontactrole ON cc_claimcontactrole.claimcontactid_stg =cc_incident.id_stg

 JOIN  DB_T_PROD_STAG.cctl_contactrole ON cctl_contactrole.id_stg=cc_claimcontactrole.role_stg

 JOIN  DB_T_PROD_STAG.cc_claimcontact ON cc_claimcontact.id_stg=cc_claimcontactrole.claimcontactid_stg

 JOIN  DB_T_PROD_STAG.cc_Contact ON cc_Contact.id_stg=cc_claimcontact.contactid_stg

 JOIN  DB_T_PROD_STAG.cctl_vehicletype ON cctl_vehicletype.id_stg=cc_incident.vehicletype_stg

 left join DB_T_PROD_STAG.cctl_contact on cctl_contact.id_stg=cc_contact.subtype_stg

WHERE 

cctl_contactrole.typecode_stg=''insured''

AND (

(cc_incident.updatetime_stg > (:START_DTTM) and cc_incident.updatetime_stg <= (:END_DTTM))

OR (cc_vehicle.updatetime_stg > (:START_DTTM) and cc_vehicle.updatetime_stg <= (:END_DTTM))

OR (cc_claim.updatetime_stg > (:START_DTTM) and cc_claim.updatetime_stg <= (:END_DTTM))

OR (cc_claimcontactrole.updatetime_stg > (:START_DTTM) and cc_claimcontactrole.updatetime_stg <= (:END_DTTM))

OR (cc_claimcontact.updatetime_stg > (:START_DTTM) and cc_claimcontact.updatetime_stg <= (:END_DTTM))

OR (cc_Contact.updatetime_stg > (:START_DTTM) and cc_Contact.updatetime_stg <= (:END_DTTM))

))C

)CC_PRTY_TO_PRTY_ASSET 

where  cc_prty_to_prty_asset.addressbookuid is not null and role_cd =''ASSET_ROLE_TYPE12''

qualify ROW_NUMBER() OVER  (partition by PolicySystemId,sbtype,classification_code,src_cd,typecode,role_cd,addressbookuid,role_dt,RateDriverclassalfa order by Updatetime desc )=1

)x) x2 where rnk=1
) SRC
)
);


-- Component exp_all_source2, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_all_source2 AS
(
SELECT
SQ_cc_prty_to_prty_asset_Owner.PolicySystemId as PolicySystemId,
SQ_cc_prty_to_prty_asset_Owner.sbtype as sbtype,
LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE */ as var_sbtype,
SQ_cc_prty_to_prty_asset_Owner.classification_code as classification_code,
LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN */ as var_classification_code,
SQ_cc_prty_to_prty_asset_Owner.typecode as typecode,
UPPER ( SQ_cc_prty_to_prty_asset_Owner.typecode ) as var_typecode,
SQ_cc_prty_to_prty_asset_Owner.role_cd as role_cd,
CASE WHEN SQ_cc_prty_to_prty_asset_Owner.role_cd = ''OWNER'' THEN ''ASSET_ROLE_TYPE12'' ELSE lower ( SQ_cc_prty_to_prty_asset_Owner.role_cd ) END as role_cd_var,
SQ_cc_prty_to_prty_asset_Owner.UPDATETIME as UPDATETIME,
LKP_3.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SRC_CD */ as v_src_cd,
DECODE ( TRUE , SQ_cc_prty_to_prty_asset_Owner.role_dt IS NULL , TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) , SQ_cc_prty_to_prty_asset_Owner.role_dt ) as var_ASSET_ROLE_DT,
var_ASSET_ROLE_DT as out_ASSET_ROLE_DT,
DECODE ( TRUE , LKP_4.PRTY_ASSET_ID /* replaced lookup LKP_PRTY_ASSET_ID */ IS NULL , 9999 , LKP_5.PRTY_ASSET_ID /* replaced lookup LKP_PRTY_ASSET_ID */ ) as var_PRTY_ASSET_ID,
var_PRTY_ASSET_ID as out_PRTY_ASSET_ID,
CASE WHEN v_src_cd != ''GWPC'' THEN ( DECODE ( TRUE , LKP_6.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_ROLE_CD */ IS NULL , ''UNK'' , LKP_7.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_ROLE_CD */ ) ) ELSE ( DECODE ( TRUE , LKP_8.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_ROLE_CD */ IS NULL , ''UNK'' , LKP_9.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_ROLE_CD */ ) ) END as var_ASSET_ROLE_CD,
var_ASSET_ROLE_CD as out_ASSET_ROLE_CD,
LKP_10.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_BUSN_CTGY */ as var_BUSN_CTGY_CD,
LKP_11.BUSN_PRTY_ID /* replaced lookup LKP_BUSN */ as var_BUSN_ID,
DECODE ( SQ_cc_prty_to_prty_asset_Owner.src_cd , ''GWPC'' , LKP_12.INDIV_PRTY_ID /* replaced lookup LKP_INDIV_CNT_MGR */ , ''GWCC'' , LKP_13.INDIV_PRTY_ID /* replaced lookup LKP_INDIV_CLM_CTR */ ) as var_INDIV_ID,
CASE WHEN var_BUSN_ID IS NULL THEN var_INDIV_ID ELSE var_BUSN_ID END as var_PRTY_ID,
CASE WHEN var_PRTY_ID IS NULL THEN 9999 ELSE var_PRTY_ID END as out_PRTY_ID,
DECODE ( TRUE , LKP_14.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_USE_CD */ IS NULL , ''UNK'' , LKP_15.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_USE_CD */ ) as var_ASSET_USE_CD,
var_ASSET_USE_CD as out_ASSET_USE_CD,
LKP_16.PRTY_ASSET_ID /* replaced lookup LKP_PRTY_TO_PRTY_ASSET */ as lkp_PRTY_ASSET_ID,
CASE WHEN SQ_cc_prty_to_prty_asset_Owner.RateDriverclassalfa IS NULL or ltrim ( rtrim ( SQ_cc_prty_to_prty_asset_Owner.RateDriverclassalfa ) ) = '''' THEN lpad ( '' '' , 50 , '' '' ) ELSE ltrim ( rtrim ( SQ_cc_prty_to_prty_asset_Owner.RateDriverclassalfa ) ) END as RateDriverclassalfa1,
LKP_17.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_LIENHOLDER_CD */ as in_lienholder_cd,
SQ_cc_prty_to_prty_asset_Owner.Retired as Retired,
--SQ_cc_prty_to_prty_asset_Owner.Rnk as Rnk,
SQ_cc_prty_to_prty_asset_Owner.source_record_id,
row_number() over (partition by SQ_cc_prty_to_prty_asset_Owner.source_record_id order by SQ_cc_prty_to_prty_asset_Owner.source_record_id) as RNK
FROM
SQ_cc_prty_to_prty_asset_Owner
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = SQ_cc_prty_to_prty_asset_Owner.sbtype
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = SQ_cc_prty_to_prty_asset_Owner.classification_code
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_SRC_CD LKP_3 ON LKP_3.SRC_IDNTFTN_VAL = SQ_cc_prty_to_prty_asset_Owner.src_cd
LEFT JOIN LKP_PRTY_ASSET_ID LKP_4 ON LKP_4.ASSET_HOST_ID_VAL = SQ_cc_prty_to_prty_asset_Owner.PolicySystemId AND LKP_4.PRTY_ASSET_SBTYPE_CD = var_sbtype AND LKP_4.PRTY_ASSET_CLASFCN_CD = var_classification_code
LEFT JOIN LKP_PRTY_ASSET_ID LKP_5 ON LKP_5.ASSET_HOST_ID_VAL = SQ_cc_prty_to_prty_asset_Owner.PolicySystemId AND LKP_5.PRTY_ASSET_SBTYPE_CD = var_sbtype AND LKP_5.PRTY_ASSET_CLASFCN_CD = var_classification_code
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_ROLE_CD LKP_6 ON LKP_6.SRC_IDNTFTN_VAL = SQ_cc_prty_to_prty_asset_Owner.typecode
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_ROLE_CD LKP_7 ON LKP_7.SRC_IDNTFTN_VAL = SQ_cc_prty_to_prty_asset_Owner.typecode
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_ROLE_CD LKP_8 ON LKP_8.SRC_IDNTFTN_VAL = role_cd_var
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_ROLE_CD LKP_9 ON LKP_9.SRC_IDNTFTN_VAL = role_cd_var
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_BUSN_CTGY LKP_10 ON LKP_10.SRC_IDNTFTN_VAL = SQ_cc_prty_to_prty_asset_Owner.typecode1
LEFT JOIN LKP_BUSN LKP_11 ON LKP_11.BUSN_CTGY_CD = var_BUSN_CTGY_CD AND LKP_11.NK_BUSN_CD = SQ_cc_prty_to_prty_asset_Owner.addressbookuid
LEFT JOIN LKP_INDIV_CNT_MGR LKP_12 ON LKP_12.NK_LINK_ID = SQ_cc_prty_to_prty_asset_Owner.addressbookuid
LEFT JOIN LKP_INDIV_CLM_CTR LKP_13 ON LKP_13.NK_PUBLC_ID = SQ_cc_prty_to_prty_asset_Owner.addressbookuid
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_USE_CD LKP_14 ON LKP_14.SRC_IDNTFTN_VAL = SQ_cc_prty_to_prty_asset_Owner.typecode
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_USE_CD LKP_15 ON LKP_15.SRC_IDNTFTN_VAL = SQ_cc_prty_to_prty_asset_Owner.typecode
LEFT JOIN LKP_PRTY_TO_PRTY_ASSET LKP_16 ON LKP_16.PRTY_ASSET_ID = var_PRTY_ASSET_ID AND LKP_16.ASSET_ROLE_CD = var_ASSET_ROLE_CD AND LKP_16.ASSET_ROLE_DTTM = var_ASSET_ROLE_DT AND LKP_16.PRTY_ID = var_PRTY_ID
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_LIENHOLDER_CD LKP_17 ON LKP_17.SRC_IDNTFTN_VAL = SQ_cc_prty_to_prty_asset_Owner.mortgageetype_alfa
QUALIFY RNK = 1
);


-- Component exp_SrcFields2, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_SrcFields2 AS
(
SELECT
exp_all_source2.out_PRTY_ASSET_ID as in_PRTY_ASSET_ID,
exp_all_source2.out_ASSET_ROLE_CD as in_ASSET_ROLE_CD,
exp_all_source2.out_ASSET_ROLE_DT as in_ASSET_ROLE_DT,
exp_all_source2.out_PRTY_ID as in_PRTY_ID,
exp_all_source2.out_ASSET_USE_CD as in_ASSET_USE_CD,
:PRCS_ID as in_PRCS_ID,
exp_all_source2.RateDriverclassalfa1 as RateDriverclassalfa,
exp_all_source2.UPDATETIME as UPDATETIME,
CURRENT_TIMESTAMP as EDW_STRT_DTTM,
TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as EDW_END_DTTm,
exp_all_source2.in_lienholder_cd as in_lienholder_cd,
exp_all_source2.Retired as Retired,
exp_all_source2.Rnk as Rnk,
exp_all_source2.sbtype as sbtype,
exp_all_source2.classification_code as classification_code,
exp_all_source2.role_cd as role_cd,
exp_all_source2.source_record_id
FROM
exp_all_source2
);


-- Component LKP_PRTY_TO_PRTY_ASSET_CDC1, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_PRTY_TO_PRTY_ASSET_CDC1 AS
(
SELECT
LKP.PRTY_ASSET_ID,
LKP.ASSET_ROLE_CD,
LKP.ASSET_ROLE_DTTM,
LKP.PRTY_ID,
LKP.ASSET_USE_CD,
LKP.DRVR_CLAS_CD,
LKP.LIENHLDR_POSITN_CD,
LKP.EDW_STRT_DTTM,
LKP.EDW_END_DTTM,
exp_SrcFields2.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_SrcFields2.source_record_id ORDER BY LKP.PRTY_ASSET_ID asc,LKP.ASSET_ROLE_CD asc,LKP.ASSET_ROLE_DTTM asc,LKP.PRTY_ID asc,LKP.ASSET_USE_CD asc,LKP.DRVR_CLAS_CD asc,LKP.LIENHLDR_POSITN_CD asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc) RNK
FROM
exp_SrcFields2
LEFT JOIN (
SELECT PRTY_TO_PRTY_ASSET.ASSET_ROLE_DTTM as ASSET_ROLE_DTTM, PRTY_TO_PRTY_ASSET.ASSET_USE_CD as ASSET_USE_CD, 
PRTY_TO_PRTY_ASSET.DRVR_CLAS_CD as DRVR_CLAS_CD, PRTY_TO_PRTY_ASSET.LIENHLDR_POSITN_CD as LIENHLDR_POSITN_CD, 
PRTY_TO_PRTY_ASSET.EDW_STRT_DTTM as EDW_STRT_DTTM, 
PRTY_TO_PRTY_ASSET.EDW_END_DTTM as EDW_END_DTTM, 
PRTY_TO_PRTY_ASSET.PRTY_ASSET_ID as PRTY_ASSET_ID, PRTY_TO_PRTY_ASSET.ASSET_ROLE_CD as ASSET_ROLE_CD, 
PRTY_TO_PRTY_ASSET.PRTY_ID as PRTY_ID FROM db_t_prod_core.PRTY_TO_PRTY_ASSET where Asset_Role_Cd=''OWNER''
QUALIFY ROW_NUMBER() OVER(PARTITION BY PRTY_TO_PRTY_ASSET.PRTY_ASSET_ID,PRTY_TO_PRTY_ASSET.ASSET_ROLE_CD, 
PRTY_TO_PRTY_ASSET.PRTY_ID ORDER BY PRTY_TO_PRTY_ASSET.EDW_END_DTTM desc) = 1
) LKP ON LKP.PRTY_ASSET_ID = exp_SrcFields2.in_PRTY_ASSET_ID AND LKP.ASSET_ROLE_CD = exp_SrcFields2.in_ASSET_ROLE_CD AND LKP.PRTY_ID = exp_SrcFields2.in_PRTY_ID
QUALIFY RNK = 1
);


-- Component exp_CDC_Check2, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_CDC_Check2 AS
(
SELECT
exp_SrcFields2.in_PRTY_ASSET_ID as in_PRTY_ASSET_ID,
exp_SrcFields2.in_ASSET_ROLE_CD as in_ASSET_ROLE_CD,
exp_SrcFields2.in_ASSET_ROLE_DT as in_ASSET_ROLE_DT,
exp_SrcFields2.in_PRTY_ID as in_PRTY_ID,
exp_SrcFields2.in_ASSET_USE_CD as in_ASSET_USE_CD,
exp_SrcFields2.in_PRCS_ID as in_PRCS_ID,
LKP_PRTY_TO_PRTY_ASSET_CDC1.PRTY_ASSET_ID as lkp_PRTY_ASSET_ID,
LKP_PRTY_TO_PRTY_ASSET_CDC1.ASSET_ROLE_CD as lkp_ASSET_ROLE_CD,
LKP_PRTY_TO_PRTY_ASSET_CDC1.ASSET_ROLE_DTTM as lkp_ASSET_ROLE_DT,
LKP_PRTY_TO_PRTY_ASSET_CDC1.PRTY_ID as lkp_PRTY_ID,
LKP_PRTY_TO_PRTY_ASSET_CDC1.ASSET_USE_CD as lkp_ASSET_USE_CD,
LKP_PRTY_TO_PRTY_ASSET_CDC1.EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
LKP_PRTY_TO_PRTY_ASSET_CDC1.EDW_END_DTTM as lkp_EDW_END_DTTM,
LKP_PRTY_TO_PRTY_ASSET_CDC1.DRVR_CLAS_CD as lkp_DRVR_CLAS_CD,
exp_SrcFields2.UPDATETIME as UPDATETIME,
exp_SrcFields2.RateDriverclassalfa as RateDriverclassalfa,
MD5 ( ltrim ( rtrim ( exp_SrcFields2.in_ASSET_ROLE_DT ) ) || ltrim ( rtrim ( exp_SrcFields2.in_ASSET_USE_CD ) ) || ltrim ( rtrim ( exp_SrcFields2.RateDriverclassalfa ) ) || ltrim ( rtrim ( exp_SrcFields2.in_lienholder_cd ) ) ) as v_SRC_MD5,
MD5 ( ltrim ( rtrim ( LKP_PRTY_TO_PRTY_ASSET_CDC1.ASSET_ROLE_DTTM ) ) || ltrim ( rtrim ( LKP_PRTY_TO_PRTY_ASSET_CDC1.ASSET_USE_CD ) ) || ltrim ( rtrim ( LKP_PRTY_TO_PRTY_ASSET_CDC1.DRVR_CLAS_CD ) ) || ltrim ( rtrim ( LKP_PRTY_TO_PRTY_ASSET_CDC1.LIENHLDR_POSITN_CD ) ) ) as v_TGT_MD5,
CASE WHEN v_TGT_MD5 IS NULL THEN ''I'' ELSE CASE WHEN v_SRC_MD5 = v_TGT_MD5 THEN ''R'' ELSE ''U'' END END as o_SRC_TGT,
exp_SrcFields2.EDW_STRT_DTTM as StartDate,
exp_SrcFields2.EDW_END_DTTm as EndDate,
exp_SrcFields2.in_lienholder_cd as in_lienholder_cd,
exp_SrcFields2.Retired as Retired,
exp_SrcFields2.Rnk as Rnk,
exp_SrcFields2.sbtype as sbtype,
exp_SrcFields2.classification_code as classification_code,
exp_SrcFields2.role_cd as role_cd,
exp_SrcFields2.source_record_id
FROM
exp_SrcFields2
INNER JOIN LKP_PRTY_TO_PRTY_ASSET_CDC1 ON exp_SrcFields2.source_record_id = LKP_PRTY_TO_PRTY_ASSET_CDC1.source_record_id
);


-- Component rtr_CDC2_Insert, Type ROUTER Output Group Insert
CREATE OR REPLACE TEMPORARY TABLE rtr_CDC2_Insert AS (
SELECT
exp_CDC_Check2.in_PRTY_ASSET_ID as in_PRTY_ASSET_ID,
exp_CDC_Check2.in_ASSET_ROLE_CD as in_ASSET_ROLE_CD,
exp_CDC_Check2.in_ASSET_ROLE_DT as in_ASSET_ROLE_DT,
exp_CDC_Check2.in_PRTY_ID as in_PRTY_ID,
exp_CDC_Check2.in_ASSET_USE_CD as in_ASSET_USE_CD,
exp_CDC_Check2.in_PRCS_ID as in_PRCS_ID,
exp_CDC_Check2.lkp_PRTY_ASSET_ID as lkp_PRTY_ASSET_ID,
exp_CDC_Check2.lkp_ASSET_ROLE_CD as lkp_ASSET_ROLE_CD,
exp_CDC_Check2.lkp_PRTY_ID as lkp_PRTY_ID,
exp_CDC_Check2.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_CDC_Check2.o_SRC_TGT as o_SRC_TGT,
exp_CDC_Check2.StartDate as StartDate,
exp_CDC_Check2.EndDate as EndDate,
exp_CDC_Check2.RateDriverclassalfa as RateDriverclassalfa,
exp_CDC_Check2.UPDATETIME as TRANS_STRT_DTTM,
exp_CDC_Check2.lkp_ASSET_ROLE_DT as lkp_ASSET_ROLE_DT,
exp_CDC_Check2.lkp_ASSET_USE_CD as lkp_ASSET_USE_CD,
exp_CDC_Check2.lkp_DRVR_CLAS_CD as lkp_DRVR_CLAS_CD,
exp_CDC_Check2.in_lienholder_cd as in_lienholder_cd,
exp_CDC_Check2.Retired as Retired,
exp_CDC_Check2.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_CDC_Check2.Rnk as Rnk,
exp_CDC_Check2.sbtype as sbtype,
exp_CDC_Check2.classification_code as classification_code,
exp_CDC_Check2.role_cd as role_cd,
exp_CDC_Check2.source_record_id
FROM
exp_CDC_Check2
WHERE exp_CDC_Check2.o_SRC_TGT = ''I'' and exp_CDC_Check2.in_PRTY_ASSET_ID <> 9999 and exp_CDC_Check2.in_PRTY_ID <> 9999 OR ( exp_CDC_Check2.Retired = 0 AND exp_CDC_Check2.lkp_EDW_END_DTTM != TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ) -- Vehicle assets with role type owner OR ( exp_CDC_Check2.o_SRC_TGT = ''U'' and exp_CDC_Check2.in_PRTY_ASSET_ID <> 9999 and exp_CDC_Check2.in_PRTY_ID <> 9999 AND exp_CDC_Check2.lkp_EDW_END_DTTM = TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AND exp_CDC_Check2.in_ASSET_ROLE_CD = ''OWNER'' and exp_CDC_Check2.role_cd = ''ASSET_ROLE_TYPE12'' and exp_CDC_Check2.sbtype = ''PRTY_ASSET_SBTYPE4'' and exp_CDC_Check2.classification_code = ''PRTY_ASSET_CLASFCN3'' ) - - Dwelling assets with role type owner OR ( exp_CDC_Check2.o_SRC_TGT = ''U'' and exp_CDC_Check2.in_PRTY_ASSET_ID <> 9999 and exp_CDC_Check2.in_PRTY_ID <> 9999 AND exp_CDC_Check2.lkp_EDW_END_DTTM = TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AND exp_CDC_Check2.in_ASSET_ROLE_CD = ''OWNER'' and exp_CDC_Check2.role_cd = ''ASSET_ROLE_TYPE12'' and exp_CDC_Check2.sbtype = ''PRTY_ASSET_SBTYPE5'' and exp_CDC_Check2.classification_code = ''PRTY_ASSET_CLASFCN1'' and exp_CDC_Check2.in_ASSET_ROLE_DT > exp_CDC_Check2.lkp_ASSET_ROLE_DT ) - - Fopdwelling assets with role type owner OR ( exp_CDC_Check2.o_SRC_TGT = ''U'' and exp_CDC_Check2.in_PRTY_ASSET_ID <> 9999 and exp_CDC_Check2.in_PRTY_ID <> 9999 AND exp_CDC_Check2.lkp_EDW_END_DTTM = TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AND exp_CDC_Check2.in_ASSET_ROLE_CD = ''OWNER'' and exp_CDC_Check2.role_cd = ''ASSET_ROLE_TYPE12'' and exp_CDC_Check2.sbtype = ''PRTY_ASSET_SBTYPE37'' and exp_CDC_Check2.classification_code = ''PRTY_ASSET_CLASFCN15'' and exp_CDC_Check2.in_ASSET_ROLE_DT > exp_CDC_Check2.lkp_ASSET_ROLE_DT )
);


-- Component rtr_CDC2_Retired, Type ROUTER Output Group Retired
CREATE OR REPLACE TEMPORARY TABLE rtr_CDC2_Retired AS (
SELECT
exp_CDC_Check2.in_PRTY_ASSET_ID as in_PRTY_ASSET_ID,
exp_CDC_Check2.in_ASSET_ROLE_CD as in_ASSET_ROLE_CD,
exp_CDC_Check2.in_ASSET_ROLE_DT as in_ASSET_ROLE_DT,
exp_CDC_Check2.in_PRTY_ID as in_PRTY_ID,
exp_CDC_Check2.in_ASSET_USE_CD as in_ASSET_USE_CD,
exp_CDC_Check2.in_PRCS_ID as in_PRCS_ID,
exp_CDC_Check2.lkp_PRTY_ASSET_ID as lkp_PRTY_ASSET_ID,
exp_CDC_Check2.lkp_ASSET_ROLE_CD as lkp_ASSET_ROLE_CD,
exp_CDC_Check2.lkp_PRTY_ID as lkp_PRTY_ID,
exp_CDC_Check2.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_CDC_Check2.o_SRC_TGT as o_SRC_TGT,
exp_CDC_Check2.StartDate as StartDate,
exp_CDC_Check2.EndDate as EndDate,
exp_CDC_Check2.RateDriverclassalfa as RateDriverclassalfa,
exp_CDC_Check2.UPDATETIME as TRANS_STRT_DTTM,
exp_CDC_Check2.lkp_ASSET_ROLE_DT as lkp_ASSET_ROLE_DT,
exp_CDC_Check2.lkp_ASSET_USE_CD as lkp_ASSET_USE_CD,
exp_CDC_Check2.lkp_DRVR_CLAS_CD as lkp_DRVR_CLAS_CD,
exp_CDC_Check2.in_lienholder_cd as in_lienholder_cd,
exp_CDC_Check2.Retired as Retired,
exp_CDC_Check2.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_CDC_Check2.Rnk as Rnk,
exp_CDC_Check2.sbtype as sbtype,
exp_CDC_Check2.classification_code as classification_code,
exp_CDC_Check2.role_cd as role_cd,
exp_CDC_Check2.source_record_id
FROM
exp_CDC_Check2
WHERE exp_CDC_Check2.o_SRC_TGT = ''R'' and exp_CDC_Check2.Retired != 0 and exp_CDC_Check2.lkp_EDW_END_DTTM = TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
);


-- Component upd_INSERT2, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_INSERT2 AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_CDC2_Insert.in_PRTY_ASSET_ID as in_PRTY_ASSET_ID1,
rtr_CDC2_Insert.in_ASSET_ROLE_CD as in_ASSET_ROLE_CD1,
rtr_CDC2_Insert.in_ASSET_ROLE_DT as in_ASSET_ROLE_DT1,
rtr_CDC2_Insert.in_PRTY_ID as in_PRTY_ID1,
rtr_CDC2_Insert.in_ASSET_USE_CD as in_ASSET_USE_CD1,
rtr_CDC2_Insert.in_PRCS_ID as in_PRCS_ID1,
rtr_CDC2_Insert.StartDate as StartDate1,
rtr_CDC2_Insert.EndDate as EndDate1,
rtr_CDC2_Insert.RateDriverclassalfa as RateDriverclassalfa1,
rtr_CDC2_Insert.TRANS_STRT_DTTM as TRANS_STRT_DTTM1,
rtr_CDC2_Insert.in_lienholder_cd as in_lienholder_cd1,
rtr_CDC2_Insert.Retired as Retired1,
rtr_CDC2_Insert.Rnk as Rnk1,
rtr_CDC2_Insert.source_record_id,
0 as UPDATE_STRATEGY_ACTION
FROM
rtr_CDC2_Insert
);


-- Component upd_retired2, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_retired2 AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_CDC2_Retired.lkp_PRTY_ASSET_ID as lkp_PRTY_ASSET_ID4,
rtr_CDC2_Retired.lkp_PRTY_ID as lkp_PRTY_ID4,
rtr_CDC2_Retired.lkp_ASSET_ROLE_CD as lkp_ASSET_ROLE_CD4,
rtr_CDC2_Retired.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM4,
rtr_CDC2_Retired.lkp_ASSET_ROLE_DT as lkp_ASSET_ROLE_DT4,
rtr_CDC2_Retired.TRANS_STRT_DTTM as TRANS_STRT_DTTM4,
rtr_CDC2_Retired.source_record_id,
1 as UPDATE_STRATEGY_ACTION
FROM
rtr_CDC2_Retired
);


-- Component exp_RETIRED2, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_RETIRED2 AS
(
SELECT
upd_retired2.lkp_PRTY_ASSET_ID4 as lkp_PRTY_ASSET_ID4,
upd_retired2.lkp_PRTY_ID4 as lkp_PRTY_ID4,
upd_retired2.lkp_ASSET_ROLE_CD4 as lkp_ASSET_ROLE_CD4,
upd_retired2.lkp_EDW_STRT_DTTM4 as lkp_EDW_STRT_DTTM4,
upd_retired2.lkp_ASSET_ROLE_DT4 as lkp_ASSET_ROLE_DT4,
upd_retired2.TRANS_STRT_DTTM4 as TRANS_STRT_DTTM41,
CURRENT_TIMESTAMP as o_EDW_END_DTTM,
upd_retired2.source_record_id
FROM
upd_retired2
);


-- Component exp_INSERT2, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_INSERT2 AS
(
SELECT
upd_INSERT2.in_PRTY_ASSET_ID1 as in_PRTY_ASSET_ID1,
upd_INSERT2.in_ASSET_ROLE_CD1 as in_ASSET_ROLE_CD1,
upd_INSERT2.in_ASSET_ROLE_DT1 as in_ASSET_ROLE_DT1,
upd_INSERT2.in_PRTY_ID1 as in_PRTY_ID1,
upd_INSERT2.in_ASSET_USE_CD1 as in_ASSET_USE_CD1,
upd_INSERT2.in_PRCS_ID1 as in_PRCS_ID1,
upd_INSERT2.RateDriverclassalfa1 as RateDriverclassalfa1,
upd_INSERT2.TRANS_STRT_DTTM1 as TRANS_STRT_DTTM1,
upd_INSERT2.in_lienholder_cd1 as in_lienholder_cd1,
CASE WHEN upd_INSERT2.Retired1 != 0 THEN CURRENT_TIMESTAMP ELSE upd_INSERT2.EndDate1 END as o_EDW_END_DTTM,
CASE WHEN upd_INSERT2.Retired1 != 0 THEN upd_INSERT2.TRANS_STRT_DTTM1 ELSE to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) END as TRANS_END_DTTM,
CASE WHEN upd_INSERT2.Retired1 = 0 THEN dateadd (second,   ( 2 * ( upd_INSERT2.Rnk1 - 1 ) ),CURRENT_TIMESTAMP  ) ELSE CURRENT_TIMESTAMP END as StartDate1,
upd_INSERT2.source_record_id
FROM
upd_INSERT2
);


-- Component PRTY_TO_PRTY_ASSET_Retired1, Type TARGET 
MERGE INTO DB_T_PROD_CORE.PRTY_TO_PRTY_ASSET
USING exp_RETIRED2 ON (PRTY_TO_PRTY_ASSET.PRTY_ASSET_ID = exp_RETIRED2.lkp_PRTY_ASSET_ID4 AND PRTY_TO_PRTY_ASSET.ASSET_ROLE_CD = exp_RETIRED2.lkp_ASSET_ROLE_CD4 AND PRTY_TO_PRTY_ASSET.PRTY_ID = exp_RETIRED2.lkp_PRTY_ID4 AND PRTY_TO_PRTY_ASSET.EDW_STRT_DTTM = exp_RETIRED2.lkp_EDW_STRT_DTTM4)
WHEN MATCHED THEN UPDATE
SET
PRTY_ASSET_ID = exp_RETIRED2.lkp_PRTY_ASSET_ID4,
ASSET_ROLE_CD = exp_RETIRED2.lkp_ASSET_ROLE_CD4,
ASSET_ROLE_DTTM = exp_RETIRED2.lkp_ASSET_ROLE_DT4,
PRTY_ID = exp_RETIRED2.lkp_PRTY_ID4,
EDW_STRT_DTTM = exp_RETIRED2.lkp_EDW_STRT_DTTM4,
EDW_END_DTTM = exp_RETIRED2.o_EDW_END_DTTM,
TRANS_END_DTTM = exp_RETIRED2.TRANS_STRT_DTTM41;


-- Component PRTY_TO_PRTY_ASSET_NewInsert1, Type TARGET 
INSERT INTO DB_T_PROD_CORE.PRTY_TO_PRTY_ASSET
(
PRTY_ASSET_ID,
ASSET_ROLE_CD,
ASSET_ROLE_DTTM,
PRTY_ID,
ASSET_USE_CD,
LIENHLDR_POSITN_CD,
DRVR_CLAS_CD,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM,
TRANS_END_DTTM
)
SELECT
exp_INSERT2.in_PRTY_ASSET_ID1 as PRTY_ASSET_ID,
exp_INSERT2.in_ASSET_ROLE_CD1 as ASSET_ROLE_CD,
exp_INSERT2.in_ASSET_ROLE_DT1 as ASSET_ROLE_DTTM,
exp_INSERT2.in_PRTY_ID1 as PRTY_ID,
exp_INSERT2.in_ASSET_USE_CD1 as ASSET_USE_CD,
exp_INSERT2.in_lienholder_cd1 as LIENHLDR_POSITN_CD,
exp_INSERT2.RateDriverclassalfa1 as DRVR_CLAS_CD,
exp_INSERT2.in_PRCS_ID1 as PRCS_ID,
exp_INSERT2.StartDate1 as EDW_STRT_DTTM,
exp_INSERT2.o_EDW_END_DTTM as EDW_END_DTTM,
exp_INSERT2.TRANS_STRT_DTTM1 as TRANS_STRT_DTTM,
exp_INSERT2.TRANS_END_DTTM as TRANS_END_DTTM
FROM
exp_INSERT2;


-- PIPELINE END FOR 2
-- Component PRTY_TO_PRTY_ASSET_NewInsert1, Type Post SQL 
UPDATE  db_t_prod_core.PRTY_TO_PRTY_ASSET   
set 

EDW_END_DTTM=A.lead1,

TRANS_END_DTTM=A.lead2

FROM

(SELECT  distinct PRTY_ASSET_ID,ASSET_ROLE_CD,PRTY_ID,EDW_STRT_DTTM,TRANS_STRT_DTTM,

max(EDW_STRT_DTTM) over (partition by PRTY_ASSET_ID,ASSET_ROLE_CD,PRTY_ID ORDER BY EDW_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND'' 

 as lead1,

max(TRANS_STRT_DTTM) over (partition by PRTY_ASSET_ID,ASSET_ROLE_CD,PRTY_ID ORDER BY EDW_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND'' 

 as lead2

FROM      db_t_prod_core.PRTY_TO_PRTY_ASSET where ASSET_ROLE_CD <> ''MRTGEE''

group by PRTY_ASSET_ID,ASSET_ROLE_CD,PRTY_ID,EDW_STRT_DTTM,TRANS_STRT_DTTM

) a


where PRTY_TO_PRTY_ASSET.EDW_STRT_DTTM = A.EDW_STRT_DTTM

AND PRTY_TO_PRTY_ASSET.PRTY_ASSET_ID=A.PRTY_ASSET_ID

and PRTY_TO_PRTY_ASSET.ASSET_ROLE_CD = A.ASSET_ROLE_CD

and PRTY_TO_PRTY_ASSET.PRTY_ID = A.PRTY_ID

and PRTY_TO_PRTY_ASSET.ASSET_ROLE_CD =''OWNER''

AND CAST(EDW_END_DTTM AS DATE)=''9999-12-31''

AND CAST(TRANS_END_DTTM AS DATE)=''9999-12-31''

and lead1 is not null;


-- PIPELINE START FOR 3

-- Component SQ_cc_prty_to_prty_asset_Mortagee, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_cc_prty_to_prty_asset_Mortagee AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as PolicySystemId,
$2 as sbtype,
$3 as classification_code,
$4 as src_cd,
$5 as typecode,
$6 as role_cd,
$7 as role_dt,
$8 as addressbookuid,
$9 as CTL_ID,
$10 as LOAD_USER,
$11 as RateDriverclassalfa,
$12 as creationdate,
$13 as expirationdate,
$14 as UPDATETIME,
$15 as mortgageetype_alfa,
$16 as Retired,
$17 as typecode1,
$18 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT DISTINCT cc_prty_to_prty_asset.PolicySystemId, cc_prty_to_prty_asset.sbtype, cc_prty_to_prty_asset.classification_code, 

cc_prty_to_prty_asset.src_cd, cc_prty_to_prty_asset.typecode, cc_prty_to_prty_asset.role_cd, cc_prty_to_prty_asset.role_dt, 

substr(cc_prty_to_prty_asset.addressbookuid,1,regexp_instr(cc_prty_to_prty_asset.addressbookuid,''-'',1)-1) as addressbookuid, 

cc_prty_to_prty_asset.CTL_ID, cc_prty_to_prty_asset.LOAD_USER, cc_prty_to_prty_asset.RateDriverclassalfa, 

cc_prty_to_prty_asset.creationdate, cc_prty_to_prty_asset.expirationdate, cc_prty_to_prty_asset.UPDATETIME, cc_prty_to_prty_asset.mortgageetype_alfa, cc_prty_to_prty_asset.Retired,

substr(cc_prty_to_prty_asset.addressbookuid,regexp_instr(cc_prty_to_prty_asset.addressbookuid,''-'',1)+1) as typecode

FROM

(SELECT * FROM(

SELECT cast(PolicySystemId as varchar(100))as PolicySystemId, sbtype, CLASSIFICATION_CODE, SRC_CD, role_cd, role_dt,TYPECODE, addressbookuid, updatetime, creationdate,

expirationdate,RateDriverclassalfa,mortgageetype_alfa,retired, CTL_ID,LOAD_USER  from 

(SELECT distinct

pc_personalvehicle.FixedID_stg as PolicySystemId,

 cast(''PRTY_ASSET_SBTYPE4'' as varchar(100))AS sbtype, 

 cast(''PRTY_ASSET_CLASFCN3'' as varchar(100))as  classification_code,

 cast(''GWPC'' as varchar(50))as src_cd, 

 cast(''ASSET_ROLE_TYPE12''as varchar(100)) AS role_cd,

 cast(case when pc_personalvehicle.PurchaseDate_Alfa_stg  is null then cast(''1900-01-01 00:00:00.000000'' AS TIMESTAMP(6)) else pc_personalvehicle.PurchaseDate_Alfa_stg end as timestamp(6)) AS role_dt,

 pctl_contact.TYPECODE_stg as typecode,

 coalesce(pc_contact.addressbookuid_stg,'''')||''-''||pctl_contact.TYPECODE_stg AS addressbookuid,

 cast(''1'' as SMALLINT) as CTL_ID,

 (:p_load_user) as LOAD_USER,

 pc_personalvehicle.UpdateTime_stg as UPDATETIME,

 pc_personalvehicle.CreateTime_stg as creationdate,

 cast(case when pc_personalvehicle.ExpirationDate_stg is null then cast(''1900-01-01'' as date  ) else 

 pc_personalvehicle.ExpirationDate_stg end as TIMESTAMP(6))as expirationdate,

pc_personalvehicle.RateDriverClass_alfa_stg as RateDriverclassalfa,

cast(null as varchar(50))as mortgageetype_alfa,

case when (pc_policyperiod.Retired_stg=0 and pc_contact.Retired_stg=0) then 0 else 1 end as Retired,

row_number () over(partition by pc_personalvehicle.fixedid_stg order by TERMNUMBER_stg DESC,ModelNumber_stg desc ) rw

FROM  DB_T_PROD_STAG.pc_personalvehicle 

JOIN  DB_T_PROD_STAG.pc_policyperiod ON pc_policyperiod.id_stg = pc_personalvehicle.branchid_stg 

JOIN DB_T_PROD_STAG.pc_policycontactrole ON pc_policyperiod.id_stg = pc_policycontactrole.branchid_stg 

JOIN DB_T_PROD_STAG.pc_contact ON pc_contact.ID_stg = pc_policycontactrole.ContactDenorm_stg 

JOIN DB_T_PROD_STAG.pctl_contact on pctl_contact.id_stg=pc_contact.Subtype_stg 

JOIN DB_T_PROD_STAG.pctl_policycontactrole ON pc_policycontactrole.Subtype_stg = pctl_policycontactrole.ID_stg AND pctl_policycontactrole.name_stg = ''PolicyPriNamedInsured''

where pc_personalvehicle.ExpirationDate_stg is null AND(

(pc_personalvehicle.updatetime_stg > (:START_DTTM) and pc_personalvehicle.updatetime_stg <= (:END_DTTM))

OR (pc_policyperiod.updatetime_stg > (:START_DTTM) and pc_policyperiod.updatetime_stg <= (:END_DTTM))

OR (pc_policycontactrole.updatetime_stg > (:START_DTTM) and pc_policycontactrole.updatetime_stg <= (:END_DTTM))

OR (pc_contact.updatetime_stg > (:START_DTTM) and pc_contact.updatetime_stg <= (:END_DTTM)))

)A WHERE RW=1 



UNION



/** Dwelling Purchased Date by Owner**/



SELECT distinct PolicySystemId, sbtype, classification_code, src_cd, role_cd, cast(case when role_dt is null then cast(''1900-01-01 00:00:00.000000'' AS TIMESTAMP(6)) else role_dt end as TIMESTAMP(6))role_dt,

typecode, addressbookuid, UpdateTime, creationdate, Expirationdate, RateDriverclassalfa, mortgageetype_alfa, retired, CTL_ID, LOAD_USER FROM (

SELECT distinct

cast(pcx_dwelling_hoe.FixedID_stg as varchar(100)) as PolicySystemId, 

cast(''PRTY_ASSET_SBTYPE5'' as varchar(100)) AS sbtype, 

cast(''PRTY_ASSET_CLASFCN1'' as varchar(100))as classification_code,

cast(''GWPC'' as varchar(50)) as src_cd, 

 cast(''ASSET_ROLE_TYPE12'' as varchar(100)) AS role_cd,

 cast(cast(cast(YearPurchased_stg as varchar(100))||''-''||trim(cast(

		case 

			when pcx_dwelling_hoe.MonthPurchased_alfa_stg in (''1'',''2'',''3'',

		''4'',''5'',''6'',''7'',''8'',''9'') then 0||trim(pcx_dwelling_hoe.MonthPurchased_alfa_stg) 

			else pcx_dwelling_hoe.MonthPurchased_alfa_stg 

		end as varchar(100))||''-''||''01'') as date)as  timestamp(6))role_dt,

pctl_contact.TYPECODE_stg as typecode,

coalesce(pc_contact.addressbookuid_stg,'''')||''-''||pctl_contact.TYPECODE_stg as  addressbookuid,

pcx_dwelling_hoe.UpdateTime_stg as UpdateTime,

cast(''1900-01-01 00:00:00.000000'' as TIMESTAMP(6)) as creationdate,

cast(''1900-01-01 00:00:00.000000'' as TIMESTAMP(6)) as Expirationdate,

cast(NULL as varchar(50)) RateDriverclassalfa,

cast(null as varchar(50)) as mortgageetype_alfa,

case when (pc_policyperiod.Retired_stg=0 and pc_contact.Retired_stg=0) then 0 else 1 end as retired,

cast(''1'' as SMALLINT) as CTL_ID,

(:p_load_user) as LOAD_USER

FROM  DB_T_PROD_STAG.pcx_dwelling_hoe 

JOIN  DB_T_PROD_STAG.pc_policyperiod ON pc_policyperiod.id_stg = pcx_dwelling_hoe.branchid_stg 

JOIN DB_T_PROD_STAG.pc_policycontactrole ON pc_policyperiod.id_stg = pc_policycontactrole.branchid_stg 

JOIN DB_T_PROD_STAG.pc_contact ON pc_contact.ID_stg = pc_policycontactrole.ContactDenorm_stg 

JOIN DB_T_PROD_STAG.pctl_contact on pctl_contact.id_stg=pc_contact.Subtype_stg 

JOIN DB_T_PROD_STAG.pctl_policycontactrole ON pc_policycontactrole.Subtype_stg = pctl_policycontactrole.ID_stg AND pctl_policycontactrole.name_stg = ''PolicyPriNamedInsured'' 

where  pcx_dwelling_hoe.ExpirationDate_stg is null/*EIM 13678*/ 

AND (

 (pcx_dwelling_hoe.updatetime_stg > (:START_DTTM) and pcx_dwelling_hoe.updatetime_stg <= (:END_DTTM))

OR (pc_policyperiod.updatetime_stg > (:START_DTTM) and pc_policyperiod.updatetime_stg <= (:END_DTTM))

OR (pc_policycontactrole.updatetime_stg > (:START_DTTM) and pc_policycontactrole.updatetime_stg <= (:END_DTTM))

OR (pc_contact.updatetime_stg > (:START_DTTM) and pc_contact.updatetime_stg <= (:END_DTTM)))





/*EIM-49111 FARM CHANGES*/

UNION

SELECT distinct

cast(pcx_fopdwelling.FixedID_stg as varchar(100)) as PolicySystemId, 

cast(''PRTY_ASSET_SBTYPE37'' as varchar(100)) AS sbtype, 

cast(''PRTY_ASSET_CLASFCN15'' as varchar(100))as classification_code,

cast(''GWPC'' as varchar(50)) as src_cd, 

 cast(''ASSET_ROLE_TYPE12'' as varchar(100)) AS role_cd,

 cast(cast(cast(YearPurchased_stg as varchar(100))||''-''||trim(cast(

        case 

            when pcx_fopdwelling.MonthPurchased_stg in (''1'',''2'',''3'',

        ''4'',''5'',''6'',''7'',''8'',''9'') then 0||trim(pcx_fopdwelling.MonthPurchased_stg) 

            else pcx_fopdwelling.MonthPurchased_stg 

        end as varchar(100))||''-''||''01'') as date)as  timestamp(6))role_dt,

pctl_contact.TYPECODE_stg as typecode,

coalesce(pc_contact.addressbookuid_stg,'''')||''-''||pctl_contact.TYPECODE_stg as  addressbookuid,

pcx_fopdwelling.UpdateTime_stg as UpdateTime,

cast(''1900-01-01 00:00:00.000000'' as TIMESTAMP(6)) as creationdate,

cast(''1900-01-01 00:00:00.000000'' as TIMESTAMP(6)) as Expirationdate,

cast(NULL as varchar(50)) RateDriverclassalfa,

cast(null as varchar(50)) as mortgageetype_alfa,

case when (pc_policyperiod.Retired_stg=0 and pc_contact.Retired_stg=0) then 0 else 1 end as retired,

cast(''1'' as SMALLINT) as CTL_ID,

(:p_load_user) as LOAD_USER

FROM  DB_T_PROD_STAG.pcx_fopdwelling 

JOIN  DB_T_PROD_STAG.pc_policyperiod ON pc_policyperiod.id_stg = pcx_fopdwelling.branchid_stg 

JOIN DB_T_PROD_STAG.pc_policycontactrole ON pc_policyperiod.id_stg = pc_policycontactrole.branchid_stg 

JOIN DB_T_PROD_STAG.pc_contact ON pc_contact.ID_stg = pc_policycontactrole.ContactDenorm_stg 

JOIN DB_T_PROD_STAG.pctl_contact on pctl_contact.id_stg=pc_contact.Subtype_stg 

JOIN DB_T_PROD_STAG.pctl_policycontactrole ON pc_policycontactrole.Subtype_stg = pctl_policycontactrole.ID_stg AND pctl_policycontactrole.name_stg = ''PolicyPriNamedInsured'' 

where (  pcx_fopdwelling.ExpirationDate_stg is null or  pcx_fopdwelling.ExpirationDate_stg>EditeffectiveDate_stg)

AND (

 (pcx_fopdwelling.updatetime_stg > (:START_DTTM) and pcx_fopdwelling.updatetime_stg <= (:END_DTTM))

OR (pc_policyperiod.updatetime_stg > (:START_DTTM) and pc_policyperiod.updatetime_stg <= (:END_DTTM))

OR (pc_policycontactrole.updatetime_stg > (:START_DTTM) and pc_policycontactrole.updatetime_stg <= (:END_DTTM))

OR (pc_contact.updatetime_stg > (:START_DTTM) and pc_contact.updatetime_stg <= (:END_DTTM)))

Qualify row_number() over(partition by pcx_fopdwelling.FixedID_stg order by coalesce(pcx_fopdwelling.expirationdate_stg, cast(''9999-12-31 23:59:59.999999'' as timestamp(6))) desc,pcx_fopdwelling.createtime_stg desc,pcx_fopdwelling.updatetime_stg desc)=1

) X

/*EIM-49111*/

UNION



select distinct 

cast(pc_personalvehicle.FixedID_stg as varchar(100))as  PolicySystemId,

cast(''PRTY_ASSET_SBTYPE4'' as varchar(100)) as sbtype, 

cast(''PRTY_ASSET_CLASFCN3'' as varchar(100))as classification_code,

cast(''GWPC'' as varchar(50))as src_cd,

pctl_additionalinteresttype.TYPECODE_stg as  role_cd,

cast(case when a.UpdateTime_stg is null then cast(''1900-01-01 00:00:00.000000'' AS TIMESTAMP(6)) else a.UpdateTime_stg end as timestamp(6))as role_dt,

pctl_contact.TYPECODE_stg as  typecode,

g.AddressBookUID_stg||''-''||pctl_contact.TYPECODE_stg as  addressbookuid,

a.updatetime_stg as UpdateTime,

cast(''1900-01-01 00:00:00.000000'' as TIMESTAMP(6)) as creationdate,

cast(''1900-01-01 00:00:00.000000'' as TIMESTAMP(6)) as Expirationdate,

pc_personalvehicle.RateDriverClass_alfa_stg as RateDriverclassalfa,

pctl_mortgageetype_alfa.TYPECODE_stg as mortgageetype_alfa,

case when (d.Retired_stg=0 and f.Retired_stg=0 and g.Retired_stg=0) then 0 else 1 end as retired,

cast(''1'' as SMALLINT) as CTL_ID,

(:p_load_user) as LOAD_USER

from DB_T_PROD_STAG.pc_addlinterestdetail a 

inner join DB_T_PROD_STAG.pc_personalvehicle on a.PAVehicle_stg=pc_personalvehicle.id_stg

inner join DB_T_PROD_STAG.pc_policycontactrole b on b.id_stg = a.PolicyAddlInterest_stg

inner join DB_T_PROD_STAG.pctl_policycontactrole c on c.id_stg = b.Subtype_stg

inner join DB_T_PROD_STAG.pc_accountcontactrole d on d.id_stg = b.AccountContactRole_stg

inner join DB_T_PROD_STAG.pctl_accountcontactrole e on e.id_stg = d.Subtype_stg

inner join DB_T_PROD_STAG.pc_accountcontact f on f.id_stg = d.AccountContact_stg

inner join DB_T_PROD_STAG.pc_contact g on g.id_stg = f.Contact_stg

inner join DB_T_PROD_STAG.pctl_additionalinteresttype on pctl_additionalinteresttype.id_stg=a.AdditionalInterestType_stg

inner join DB_T_PROD_STAG.pctl_contact on pctl_contact.id_stg=g.Subtype_stg

left outer join DB_T_PROD_STAG.pctl_mortgageetype_alfa on a.MortgageeType_alfa_stg=pctl_mortgageetype_alfa.ID_stg

where e.TYPECODE_stg = ''AdditionalInterest'' and pctl_additionalinteresttype.TYPECODE_stg not in (''LHFirst_alfa'') 

AND (

(a.updatetime_stg > (:START_DTTM) and a.updatetime_stg <= (:END_DTTM))

OR (pc_personalvehicle.updatetime_stg > (:START_DTTM) and pc_personalvehicle.updatetime_stg <= (:END_DTTM))

OR (b.updatetime_stg > (:START_DTTM) and b.updatetime_stg <= (:END_DTTM))

OR (d.updatetime_stg > (:START_DTTM) and d.updatetime_stg <= (:END_DTTM))

OR (f.updatetime_stg > (:START_DTTM) and f.updatetime_stg <= (:END_DTTM))

OR (g.updatetime_stg > (:START_DTTM) and g.updatetime_stg <= (:END_DTTM)))



UNION



/** Lienholder Info for Dwelling**/

select distinct 

cast(pcx_dwelling_hoe.FixedID_stg as varchar(100))as PolicySystemId ,

cast(''PRTY_ASSET_SBTYPE5'' as varchar(100)) AS sbtype,

cast(''PRTY_ASSET_CLASFCN1'' as varchar(100))as classification_code,

cast(''GWPC''as varchar(50)) as src_cd, 

pctl_additionalinteresttype.TYPECODE_stg as role_cd,

CAST(case when a.UpdateTime_stg is null then cast(''1900-01-01 00:00:00.000000'' AS TIMESTAMP(6)) else a.UpdateTime_stg end as TIMESTAMP(6))as role_dt,

pctl_contact.TYPECODE_stg as typecode,

g.AddressBookUID_stg||''-''||pctl_contact.TYPECODE_stg as addressbookuid,

a.updatetime_stg as updatetime,

cast(''1900-01-01 00:00:00.000000'' as TIMESTAMP(6)) as creationdate,

cast(''1900-01-01 00:00:00.000000'' as TIMESTAMP(6)) as Expirationdate,

cast(NULL as varchar(50))as RateDriverclassalfa,

pctl_mortgageetype_alfa.TYPECODE_stg as mortgageetype_alfa,

case when (d.Retired_stg=0 and f.Retired_stg=0 and g.Retired_stg=0) then 0 else 1 end as retired,

cast(''1'' as SMALLINT) as CTL_ID,

(:p_load_user) as LOAD_USER

from DB_T_PROD_STAG.pc_addlinterestdetail a 

inner join DB_T_PROD_STAG.pcx_dwelling_hoe on a.Dwelling_stg=pcx_dwelling_hoe.id_stg

inner join DB_T_PROD_STAG.pc_policycontactrole b on b.id_stg = a.PolicyAddlInterest_stg

inner join DB_T_PROD_STAG.pctl_policycontactrole c on c.id_stg = b.Subtype_stg

inner join DB_T_PROD_STAG.pc_accountcontactrole d on d.id_stg = b.AccountContactRole_stg

inner join DB_T_PROD_STAG.pctl_accountcontactrole e on e.id_stg = d.Subtype_stg

inner join DB_T_PROD_STAG.pc_accountcontact f on f.id_stg = d.AccountContact_stg

inner join DB_T_PROD_STAG.pc_contact g on g.id_stg = f.Contact_stg

inner join DB_T_PROD_STAG.pctl_additionalinteresttype on pctl_additionalinteresttype.id_stg=a.AdditionalInterestType_stg

inner join DB_T_PROD_STAG.pctl_contact on pctl_contact.id_stg=g.Subtype_stg

left outer join DB_T_PROD_STAG.pctl_mortgageetype_alfa on a.MortgageeType_alfa_stg=pctl_mortgageetype_alfa.ID_stg

where e.TYPECODE_stg = ''AdditionalInterest''

AND (

(a.updatetime_stg > (:START_DTTM) and a.updatetime_stg <= (:END_DTTM))

OR (pcx_dwelling_hoe.updatetime_stg > (:START_DTTM) and pcx_dwelling_hoe.updatetime_stg <= (:END_DTTM))

OR (b.updatetime_stg > (:START_DTTM) and b.updatetime_stg <= (:END_DTTM))

OR (d.updatetime_stg > (:START_DTTM) and d.updatetime_stg <= (:END_DTTM))

OR (f.updatetime_stg > (:START_DTTM) and f.updatetime_stg <= (:END_DTTM))

OR (g.updatetime_stg > (:START_DTTM) and g.updatetime_stg <= (:END_DTTM))

))A

union all



SELECT * FROM (

SELECT DISTINCT cast(id as varchar(100))as PolicySystemId, sbtype, classification_code, src_cd, role_cd, CAST(case when roledate is null then cast(''1900-01-01 00:00:00.000000'' AS TIMESTAMP(6)) else roledate end as TIMESTAMP(6))as role_dt, TYPECODE, addressbookuid, updatetime, 

creationdate, expirationdate, RateDriverclassalfa, mortgageetype_alfa,retired, CTL_ID, LOAD_USER FROM( 

select distinct  id,sbtype,classification_code,src_cd,role_cd,

(case when (cast(lag_exp as date)=cast(''9999-12-31'' as date)) then coalesce(lag_eff,roledt,min(periodstart) over (partition by id,addressbookuid order by termnumber,modelnumber,updatetime)) else coalesce(roledt,lag_eff,periodstart) end) roledate,

TYPECODE,

addressbookuid,

updatetime,

cast(creationdate as timestamp(6))as creationdate , 

cast(expirationdate as timestamp(6))as expirationdate,

RateDriverclassalfa,

mortgageetype_alfa,

retired,cast(''1'' as SMALLINT) as CTL_ID,

(:p_load_user) as LOAD_USER from

(

select distinct id,sbtype,addressbookuid,classification_code,src_cd,role_cd,roledt,updatetime,TYPECODE,expirationdate,periodstart,effectivedate,

lag(roledt) over(partition by id,addressbookuid order by termnumber,modelnumber,updatetime) lag_eff, lag_exp,

creationdate, RateDriverclassalfa,

mortgageetype_alfa,

retired,start_dttm,end_dttm,termnumber,modelnumber

from

(

select distinct

id,sbtype,classification_code,src_cd,role_cd,periodstart,effectivedate,

(case when effectivedate is null then (lag(effectivedate) over(partition by id,addressbookuid order by termnumber,modelnumber,updatetime)) else effectivedate end) roledt,

TYPECODE,addressbookuid,updatetime,creationdate,

expirationdate, lag(expirationdate) over (order by id,updatetime)lag_exp,

RateDriverclassalfa,

mortgageetype_alfa,

retired,start_dttm,end_dttm,termnumber,modelnumber,rnk

from

(

select distinct id,

cast(''PRTY_ASSET_SBTYPE4'' as varchar(100)) as sbtype ,

cast(''PRTY_ASSET_CLASFCN3'' as varchar(100)) as classification_code,

cast(''GWPC'' as varchar(50))as src_cd,

 termnumber, modelnumber, addressbookuid,TYPECODE,role_cd,mortgageetype_alfa,

(case when (effectivedate is null and rnk=1 ) then periodstart else effectivedate end) as effectivedate, 

(case when (expirationdate is null) then cast(''9999-12-31'' as date)  else expirationdate end) as expirationdate,updatetime, RateDriverclassalfa,

cast(''1900-01-01'' as date ) as creationdate,cast (''1900-01-01'' as date  ) as start_dttm,

cast (''9999-01-01'' as date  ) as end_dttm,

periodstart,periodend,retired,rnk from

(

select distinct  id, termnumber, modelnumber, updatetime, RateDriverclassalfa,

effectivedate, expirationdate,addressbookuid,TYPECODE,role_cd, mortgageetype_alfa,

  periodstart, periodend,retired,

rank() over(partition by id,addressbookuid order by termnumber,modelnumber,updatetime) rnk from

(

select distinct pc_personalvehicle.FixedID_stg as id, 

pol.termnumber_stg as termnumber,

pol.modelnumber_stg as  modelnumber, addl.updatetime_stg as  updatetime, pc_personalvehicle.RateDriverClass_alfa_stg as  RateDriverclassalfa,

addl.effectivedate_stg as  effectivedate, pctl_contact.TYPECODE_stg as TYPECODE,pctl_additionalinteresttype.TYPECODE_stg as role_cd,

addl.expirationdate_stg as expirationdate, g.AddressBookUID_stg||''-''||pctl_contact.TYPECODE_stg as addressbookuid, pctl_mortgageetype_alfa.TYPECODE_stg as mortgageetype_alfa, case when (d.Retired_stg=0 and f.Retired_stg=0 and g.Retired_stg=0) then 0 else 1 end as retired,

pol.periodStart_stg as periodstart,

pol.periodEnd_stg as periodend

from DB_T_PROD_STAG.pc_addlinterestdetail addl 

inner JOIN  DB_T_PROD_STAG.pc_policyperiod pol ON  pol.id_stg=addl.BranchID_stg

inner join DB_T_PROD_STAG.pc_personalvehicle on addl.PAVehicle_stg=pc_personalvehicle.id_stg

inner join DB_T_PROD_STAG.pc_policycontactrole b on b.id_stg = addl.PolicyAddlInterest_stg

inner join DB_T_PROD_STAG.pctl_policycontactrole c on c.id_stg = b.Subtype_stg

inner join DB_T_PROD_STAG.pc_accountcontactrole d on d.id_stg = b.AccountContactRole_stg

inner join DB_T_PROD_STAG.pctl_accountcontactrole e on e.id_Stg = d.Subtype_stg

inner join DB_T_PROD_STAG.pc_accountcontact f on f.id_stg = d.AccountContact_stg

inner join DB_T_PROD_STAG.pc_contact g on g.id_stg = f.Contact_stg

inner join DB_T_PROD_STAG.pctl_additionalinteresttype on pctl_additionalinteresttype.id_stg=addl.AdditionalInterestType_stg

inner join DB_T_PROD_STAG.pctl_contact on pctl_contact.id_stg=g.Subtype_stg

left outer join DB_T_PROD_STAG.pctl_mortgageetype_alfa on addl.MortgageeType_alfa_stg=pctl_mortgageetype_alfa.ID_stg

where

e.TYPECODE_stg = ''AdditionalInterest''  

and pctl_additionalinteresttype.TYPECODE_stg in (''LHFirst_alfa'') 

AND (

(addl.updatetime_stg > (:START_DTTM) and addl.updatetime_stg <= (:END_DTTM))

OR (pol.updatetime_stg > (:START_DTTM) and pol.updatetime_stg <= (:END_DTTM))

OR (pc_personalvehicle.updatetime_stg > (:START_DTTM) and pc_personalvehicle.updatetime_stg <= (:END_DTTM))

OR (b.updatetime_stg > (:START_DTTM) and b.updatetime_stg <= (:END_DTTM))

OR (d.updatetime_stg > (:START_DTTM) and d.updatetime_stg <= (:END_DTTM))

OR (f.updatetime_stg > (:START_DTTM) and f.updatetime_stg <= (:END_DTTM))

OR (g.updatetime_stg > (:START_DTTM) and g.updatetime_stg <= (:END_DTTM))

)

    ) innr_mst

       )rnk

   )lag_inr

   )lag_col

    )final)X)B

	

	union all

	

	SELECT * FROM(

select distinct 

case when cc_vehicle.PolicySystemId_stg is not null then SUBSTR(cc_vehicle.policysystemid_stg, POSITION('':''in cc_vehicle.policysystemid_stg)+1,LENGTH(cc_vehicle.policysystemid_stg))

when (cc_vehicle.PolicySystemId_stg is null and cc_vehicle.Vin_stg is not null) then  ''VIN:''||cc_vehicle.vin_stg 

when (cc_vehicle.PolicySystemId_stg is null and cc_vehicle.Vin_stg is null and cc_vehicle.LicensePlate_stg is not null) then ''LP:''||cc_vehicle.licenseplate_stg

when (cc_vehicle.PolicySystemId_stg is null and cc_vehicle.Vin_stg is null and cc_vehicle.LicensePlate_stg is null) then cc_vehicle.PublicID_stg

end as PolicySystemId,

cast(''PRTY_ASSET_SBTYPE4'' as varchar(100)) as sbtype ,

cast(''PRTY_ASSET_CLASFCN3'' as varchar(100)) as classification_code,

case when PolicySystemId_stg is null then cast(''GWCC'' as varchar(100))else cast(''GWPC'' as varchar(100)) end as src_cd,

cast(''ASSET_ROLE_TYPE9'' as varchar(100)) as role_cd,

cast(case when cc_incident.DateSalvageAssigned_stg is null then cast(''1900-01-01 00:00:00.000000'' AS TIMESTAMP(6)) else cc_incident.DateSalvageAssigned_stg end  as TIMESTAMP(6))as role_dt,

cast(''BUSN_CTGY5'' as varchar(100))as typecode,

cctl_salvageyard_alfa.TYPECODE_stg||''-''||cast(''BUSN_CTGY5'' as varchar(100))as addressbookuid,

cc_incident.updatetime_stg as UPDATETIME,

cast(''1900-01-01 00:00:00.000000'' as TIMESTAMP(6)) as creationdate,

cast(''1900-01-01 00:00:00.000000'' as TIMESTAMP(6)) as Expirationdate,

cast(NULL as varchar(50)) as  RateDriverclassalfa,

cast(null as varchar(50)) as mortgageetype_alfa,

case when (cc_incident.Retired_stg=0 and cc_vehicle.Retired_stg=0 ) 

then 0 else 1 end  as retired,

cast(''1'' as SMALLINT) as CTL_ID,

(:p_load_user) as LOAD_USER

from DB_T_PROD_STAG.cc_incident 

inner join DB_T_PROD_STAG.cc_vehicle on cc_incident.VehicleID_stg = cc_vehicle.ID_stg

left outer join DB_T_PROD_STAG.cctl_incident on cc_incident.Subtype_stg = cctl_incident.id_stg

left join DB_T_PROD_STAG.cctl_salvageyard_alfa on cctl_salvageyard_alfa.id_stg=cc_incident.SalvageYard_alfa_stg

where

(cc_incident.updatetime_stg > (:START_DTTM) and cc_incident.updatetime_stg <= (:END_DTTM))

OR (cc_vehicle.updatetime_stg > (:START_DTTM) and cc_vehicle.updatetime_stg <= (:END_DTTM))





Union



select distinct 

/** Party Asset **/

case when (cc_vehicle.PolicySystemId_stg is null and cc_vehicle.Vin_stg is not null) then  ''VIN:''||cc_vehicle.vin_stg 

when (cc_vehicle.PolicySystemId_stg is null and cc_vehicle.Vin_stg is null and cc_vehicle.LicensePlate_stg is not null) then ''LP:''||cc_vehicle.licenseplate_stg

when (cc_vehicle.PolicySystemId_stg is null and cc_vehicle.Vin_stg is null and cc_vehicle.LicensePlate_stg is null) then cc_vehicle.PublicID_stg

end as PolicySystemId,

cast(''PRTY_ASSET_SBTYPE4'' as varchar(100))as sbtype,

cast(''PRTY_ASSET_CLASFCN3'' as  varchar(100))as classification_code,

case when cc_vehicle.PolicySystemId_stg is null then cast(''GWCC''as varchar(100)) else cast(''GWPC'' as varchar(100)) end as src_cd,

cctl_vehicletype.typecode_stg as role_cd,

cast(case when cc_Claim.LossDate_stg is null then cast(''1900-01-01 00:00:00.000000'' AS TIMESTAMP(6)) else cc_Claim.LossDate_stg end as TIMESTAMP(6))as role_dt, 

cctl_reasonforuse.TYPECODE_stg  as TYPECODE,

cc_contact.PublicID_stg||''-''||cctl_contact.typecode_stg as addressbookuid, 

cc_claimcontactrole.UpdateTime_stg as UpdateTime,

cast(''1900-01-01 00:00:00.000000'' as TIMESTAMP(6)) as creationdate,

cast(''1900-01-01 00:00:00.000000'' as TIMESTAMP(6)) as Expirationdate,

cast(NULL as varchar(50))as RateDriverclassalfa,

cast(null as varchar(50)) as mortgageetype_alfa,

case when (cc_incident.Retired_stg=0 and cc_vehicle.Retired_stg =0 

and cc_claim.Retired_stg=0 and cc_claimcontactrole.Retired_stg=0 and cc_claimcontact.Retired_stg=0 

and cc_contact.Retired_stg=0 ) 

then 0 else 1 end as retired,

cast(''1'' as SMALLINT) as CTL_ID,

(:p_load_user) as LOAD_USER

FROM DB_T_PROD_STAG.cc_incident 

inner join DB_T_PROD_STAG.cc_vehicle on cc_incident.VehicleID_stg = cc_vehicle.ID_stg

left outer join DB_T_PROD_STAG.cctl_incident on cc_incident.Subtype_stg = cctl_incident.id_stg

LEFT JOIN  DB_T_PROD_STAG.cctl_reasonforuse ON cctl_reasonforuse.ID_stg = cc_incident.VehicleUseReason_stg 

join (select cc_claim.id_stg, cc_claim.State_stg, cc_claim.updatetime_stg, cc_claim.Retired_stg, cc_claim.LossDate_stg from DB_T_PROD_STAG.cc_Claim inner join DB_T_PROD_STAG.cctl_claimstate on cc_claim.State_stg= cctl_claimstate.id_stg where cctl_claimstate.name_stg <> ''Draft'') cc_Claim on cc_claim.id_stg=cc_incident.ClaimID_stg

 JOIN  DB_T_PROD_STAG.cc_claimcontactrole ON cc_claimcontactrole.claimcontactid_stg =cc_incident.id_stg

 JOIN  DB_T_PROD_STAG.cctl_contactrole ON cctl_contactrole.id_stg=cc_claimcontactrole.role_stg

 JOIN  DB_T_PROD_STAG.cc_claimcontact ON cc_claimcontact.id_stg=cc_claimcontactrole.claimcontactid_stg

 JOIN  DB_T_PROD_STAG.cc_Contact ON cc_Contact.id_stg=cc_claimcontact.contactid_stg

 JOIN  DB_T_PROD_STAG.cctl_vehicletype ON cctl_vehicletype.id_stg=cc_incident.vehicletype_stg

 left join DB_T_PROD_STAG.cctl_contact on cctl_contact.id_stg=cc_contact.subtype_stg

WHERE 

cctl_contactrole.typecode_stg=''insured''

AND (

(cc_incident.updatetime_stg > (:START_DTTM) and cc_incident.updatetime_stg <= (:END_DTTM))

OR (cc_vehicle.updatetime_stg > (:START_DTTM) and cc_vehicle.updatetime_stg <= (:END_DTTM))

OR (cc_claim.updatetime_stg > (:START_DTTM) and cc_claim.updatetime_stg <= (:END_DTTM))

OR (cc_claimcontactrole.updatetime_stg > (:START_DTTM) and cc_claimcontactrole.updatetime_stg <= (:END_DTTM))

OR (cc_claimcontact.updatetime_stg > (:START_DTTM) and cc_claimcontact.updatetime_stg <= (:END_DTTM))

OR (cc_Contact.updatetime_stg > (:START_DTTM) and cc_Contact.updatetime_stg <= (:END_DTTM))

))C)cc_prty_to_prty_asset 

where  cc_prty_to_prty_asset.addressbookuid is not null and role_cd=''MORTGAGEE'' 

qualify ROW_NUMBER() OVER  (partition by PolicySystemId,sbtype,classification_code,src_cd,typecode,role_cd,addressbookuid,mortgageetype_alfa order by UPDATETIME desc )=1

/* and PolicySystemId =''276'' and role_cd=''OWNER''  */
 order by PolicySystemId,sbtype,classification_code,role_dt,UPDATETIME asc
) SRC
)
);


-- Component exp_all_source1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_all_source1 AS
(
SELECT
SQ_cc_prty_to_prty_asset_Mortagee.PolicySystemId as PolicySystemId,
LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE */ as var_sbtype,
LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN */ as var_classification_code,
SQ_cc_prty_to_prty_asset_Mortagee.typecode as typecode,
UPPER ( SQ_cc_prty_to_prty_asset_Mortagee.typecode ) as var_typecode,
CASE WHEN SQ_cc_prty_to_prty_asset_Mortagee.role_cd = ''OWNER'' THEN ''ASSET_ROLE_TYPE12'' ELSE lower ( SQ_cc_prty_to_prty_asset_Mortagee.role_cd ) END as role_cd_var,
SQ_cc_prty_to_prty_asset_Mortagee.UPDATETIME as UPDATETIME,
LKP_3.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SRC_CD */ as v_src_cd,
DECODE ( TRUE , SQ_cc_prty_to_prty_asset_Mortagee.role_dt IS NULL , TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) , SQ_cc_prty_to_prty_asset_Mortagee.role_dt ) as var_ASSET_ROLE_DT,
var_ASSET_ROLE_DT as out_ASSET_ROLE_DT,
DECODE ( TRUE , LKP_4.PRTY_ASSET_ID /* replaced lookup LKP_PRTY_ASSET_ID */ IS NULL , 9999 , LKP_5.PRTY_ASSET_ID /* replaced lookup LKP_PRTY_ASSET_ID */ ) as var_PRTY_ASSET_ID,
var_PRTY_ASSET_ID as out_PRTY_ASSET_ID,
CASE WHEN v_src_cd != ''GWPC'' THEN ( DECODE ( TRUE , LKP_6.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_ROLE_CD */ IS NULL , ''UNK'' , LKP_7.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_ROLE_CD */ ) ) ELSE ( DECODE ( TRUE , LKP_8.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_ROLE_CD */ IS NULL , ''UNK'' , LKP_9.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_ROLE_CD */ ) ) END as var_ASSET_ROLE_CD,
var_ASSET_ROLE_CD as out_ASSET_ROLE_CD,
LKP_10.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_BUSN_CTGY */ as var_BUSN_CTGY_CD,
LKP_11.BUSN_PRTY_ID /* replaced lookup LKP_BUSN */ as var_BUSN_ID,
DECODE ( SQ_cc_prty_to_prty_asset_Mortagee.src_cd , ''GWPC'' , LKP_12.INDIV_PRTY_ID /* replaced lookup LKP_INDIV_CNT_MGR */ , ''GWCC'' , LKP_13.INDIV_PRTY_ID /* replaced lookup LKP_INDIV_CLM_CTR */ ) as var_INDIV_ID,
CASE WHEN var_BUSN_ID IS NULL THEN var_INDIV_ID ELSE var_BUSN_ID END as var_PRTY_ID,
CASE WHEN var_PRTY_ID IS NULL THEN 9999 ELSE var_PRTY_ID END as out_PRTY_ID,
DECODE ( TRUE , LKP_14.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_USE_CD */ IS NULL , ''UNK'' , LKP_15.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_USE_CD */ ) as var_ASSET_USE_CD,
var_ASSET_USE_CD as out_ASSET_USE_CD,
LKP_16.PRTY_ASSET_ID /* replaced lookup LKP_PRTY_TO_PRTY_ASSET */ as lkp_PRTY_ASSET_ID,
CASE WHEN SQ_cc_prty_to_prty_asset_Mortagee.RateDriverclassalfa IS NULL or ltrim ( rtrim ( SQ_cc_prty_to_prty_asset_Mortagee.RateDriverclassalfa ) ) = '''' THEN lpad ( '' '' , 50 , '' '' ) ELSE ltrim ( rtrim ( SQ_cc_prty_to_prty_asset_Mortagee.RateDriverclassalfa ) ) END as RateDriverclassalfa1,
LKP_17.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_LIENHOLDER_CD */ as in_lienholder_cd,
SQ_cc_prty_to_prty_asset_Mortagee.Retired as Retired,
SQ_cc_prty_to_prty_asset_Mortagee.source_record_id,
row_number() over (partition by SQ_cc_prty_to_prty_asset_Mortagee.source_record_id order by SQ_cc_prty_to_prty_asset_Mortagee.source_record_id) as RNK
FROM
SQ_cc_prty_to_prty_asset_Mortagee
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = SQ_cc_prty_to_prty_asset_Mortagee.sbtype
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = SQ_cc_prty_to_prty_asset_Mortagee.classification_code
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_SRC_CD LKP_3 ON LKP_3.SRC_IDNTFTN_VAL = SQ_cc_prty_to_prty_asset_Mortagee.src_cd
LEFT JOIN LKP_PRTY_ASSET_ID LKP_4 ON LKP_4.ASSET_HOST_ID_VAL = SQ_cc_prty_to_prty_asset_Mortagee.PolicySystemId AND LKP_4.PRTY_ASSET_SBTYPE_CD = var_sbtype AND LKP_4.PRTY_ASSET_CLASFCN_CD = var_classification_code
LEFT JOIN LKP_PRTY_ASSET_ID LKP_5 ON LKP_5.ASSET_HOST_ID_VAL = SQ_cc_prty_to_prty_asset_Mortagee.PolicySystemId AND LKP_5.PRTY_ASSET_SBTYPE_CD = var_sbtype AND LKP_5.PRTY_ASSET_CLASFCN_CD = var_classification_code
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_ROLE_CD LKP_6 ON LKP_6.SRC_IDNTFTN_VAL = SQ_cc_prty_to_prty_asset_Mortagee.typecode
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_ROLE_CD LKP_7 ON LKP_7.SRC_IDNTFTN_VAL = SQ_cc_prty_to_prty_asset_Mortagee.typecode
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_ROLE_CD LKP_8 ON LKP_8.SRC_IDNTFTN_VAL = role_cd_var
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_ROLE_CD LKP_9 ON LKP_9.SRC_IDNTFTN_VAL = role_cd_var
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_BUSN_CTGY LKP_10 ON LKP_10.SRC_IDNTFTN_VAL = SQ_cc_prty_to_prty_asset_Mortagee.typecode1
LEFT JOIN LKP_BUSN LKP_11 ON LKP_11.BUSN_CTGY_CD = var_BUSN_CTGY_CD AND LKP_11.NK_BUSN_CD = SQ_cc_prty_to_prty_asset_Mortagee.addressbookuid
LEFT JOIN LKP_INDIV_CNT_MGR LKP_12 ON LKP_12.NK_LINK_ID = SQ_cc_prty_to_prty_asset_Mortagee.addressbookuid
LEFT JOIN LKP_INDIV_CLM_CTR LKP_13 ON LKP_13.NK_PUBLC_ID = SQ_cc_prty_to_prty_asset_Mortagee.addressbookuid
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_USE_CD LKP_14 ON LKP_14.SRC_IDNTFTN_VAL = SQ_cc_prty_to_prty_asset_Mortagee.typecode
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_USE_CD LKP_15 ON LKP_15.SRC_IDNTFTN_VAL = SQ_cc_prty_to_prty_asset_Mortagee.typecode
LEFT JOIN LKP_PRTY_TO_PRTY_ASSET LKP_16 ON LKP_16.PRTY_ASSET_ID = var_PRTY_ASSET_ID AND LKP_16.ASSET_ROLE_CD = var_ASSET_ROLE_CD AND LKP_16.ASSET_ROLE_DTTM = var_ASSET_ROLE_DT AND LKP_16.PRTY_ID = var_PRTY_ID
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_LIENHOLDER_CD LKP_17 ON LKP_17.SRC_IDNTFTN_VAL = SQ_cc_prty_to_prty_asset_Mortagee.mortgageetype_alfa
QUALIFY RNK = 1
);


-- Component exp_SrcFields1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_SrcFields1 AS
(
SELECT
exp_all_source1.out_PRTY_ASSET_ID as in_PRTY_ASSET_ID,
exp_all_source1.out_ASSET_ROLE_CD as in_ASSET_ROLE_CD,
exp_all_source1.out_ASSET_ROLE_DT as in_ASSET_ROLE_DT,
exp_all_source1.out_PRTY_ID as in_PRTY_ID,
exp_all_source1.out_ASSET_USE_CD as in_ASSET_USE_CD,
:PRCS_ID as in_PRCS_ID,
exp_all_source1.RateDriverclassalfa1 as RateDriverclassalfa,
exp_all_source1.UPDATETIME as UPDATETIME,
CURRENT_TIMESTAMP as EDW_STRT_DTTM,
TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as EDW_END_DTTm,
dateadd ( second, -1, CURRENT_TIMESTAMP  ) as EDW_expiry,
exp_all_source1.in_lienholder_cd as in_lienholder_cd,
exp_all_source1.Retired as Retired,
exp_all_source1.source_record_id
FROM
exp_all_source1
);


-- Component LKP_PRTY_TO_PRTY_ASSET_CDC_MORTGAGEE, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_PRTY_TO_PRTY_ASSET_CDC_MORTGAGEE AS
(
SELECT
LKP.PRTY_ASSET_ID,
LKP.ASSET_ROLE_CD,
LKP.ASSET_ROLE_DTTM,
LKP.PRTY_ID,
LKP.ASSET_USE_CD,
LKP.DRVR_CLAS_CD,
LKP.EDW_STRT_DTTM,
LKP.EDW_END_DTTM,
exp_SrcFields1.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_SrcFields1.source_record_id ORDER BY LKP.PRTY_ASSET_ID asc,LKP.ASSET_ROLE_CD asc,LKP.ASSET_ROLE_DTTM asc,LKP.PRTY_ID asc,LKP.ASSET_USE_CD asc,LKP.DRVR_CLAS_CD asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc) RNK
FROM
exp_SrcFields1
LEFT JOIN (
SELECT	PRTY_TO_PRTY_ASSET.ASSET_ROLE_DTTM as ASSET_ROLE_DTTM, PRTY_TO_PRTY_ASSET.ASSET_USE_CD as ASSET_USE_CD,
		PRTY_TO_PRTY_ASSET.DRVR_CLAS_CD as DRVR_CLAS_CD, PRTY_TO_PRTY_ASSET.LIENHLDR_POSITN_CD as LIENHLDR_POSITN_CD,
		PRTY_TO_PRTY_ASSET.EDW_STRT_DTTM as EDW_STRT_DTTM,
		PRTY_TO_PRTY_ASSET.EDW_END_DTTM as EDW_END_DTTM,
		PRTY_TO_PRTY_ASSET.PRTY_ASSET_ID as PRTY_ASSET_ID, PRTY_TO_PRTY_ASSET.ASSET_ROLE_CD as ASSET_ROLE_CD,
		PRTY_TO_PRTY_ASSET.PRTY_ID as PRTY_ID 
FROM	db_t_prod_core.PRTY_TO_PRTY_ASSET 
where	Asset_Role_Cd=''MRTGEE''
QUALIFY	ROW_NUMBER() OVER(
PARTITION BY PRTY_TO_PRTY_ASSET.PRTY_ASSET_ID,PRTY_TO_PRTY_ASSET.ASSET_ROLE_CD,
		PRTY_TO_PRTY_ASSET.PRTY_ID,PRTY_TO_PRTY_ASSET.LIENHLDR_POSITN_CD 
ORDER BY PRTY_TO_PRTY_ASSET.EDW_END_DTTM desc) = 1
) LKP ON LKP.PRTY_ASSET_ID = exp_SrcFields1.in_PRTY_ASSET_ID AND LKP.ASSET_ROLE_CD = exp_SrcFields1.in_ASSET_ROLE_CD AND LKP.PRTY_ID = exp_SrcFields1.in_PRTY_ID AND LKP.LIENHLDR_POSITN_CD = exp_SrcFields1.in_lienholder_cd
QUALIFY RNK = 1
);


-- Component exp_CDC_Check1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_CDC_Check1 AS
(
SELECT
exp_SrcFields1.in_PRTY_ASSET_ID as in_PRTY_ASSET_ID,
exp_SrcFields1.in_ASSET_ROLE_CD as in_ASSET_ROLE_CD,
exp_SrcFields1.in_ASSET_ROLE_DT as in_ASSET_ROLE_DT,
exp_SrcFields1.in_PRTY_ID as in_PRTY_ID,
exp_SrcFields1.in_ASSET_USE_CD as in_ASSET_USE_CD,
exp_SrcFields1.in_PRCS_ID as in_PRCS_ID,
LKP_PRTY_TO_PRTY_ASSET_CDC_MORTGAGEE.PRTY_ASSET_ID as lkp_PRTY_ASSET_ID,
LKP_PRTY_TO_PRTY_ASSET_CDC_MORTGAGEE.ASSET_ROLE_CD as lkp_ASSET_ROLE_CD,
LKP_PRTY_TO_PRTY_ASSET_CDC_MORTGAGEE.ASSET_ROLE_DTTM as lkp_ASSET_ROLE_DT,
LKP_PRTY_TO_PRTY_ASSET_CDC_MORTGAGEE.PRTY_ID as lkp_PRTY_ID,
LKP_PRTY_TO_PRTY_ASSET_CDC_MORTGAGEE.ASSET_USE_CD as lkp_ASSET_USE_CD,
LKP_PRTY_TO_PRTY_ASSET_CDC_MORTGAGEE.EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
LKP_PRTY_TO_PRTY_ASSET_CDC_MORTGAGEE.EDW_END_DTTM as lkp_EDW_END_DTTM,
LKP_PRTY_TO_PRTY_ASSET_CDC_MORTGAGEE.DRVR_CLAS_CD as lkp_DRVR_CLAS_CD,
exp_SrcFields1.UPDATETIME as UPDATETIME,
exp_SrcFields1.RateDriverclassalfa as RateDriverclassalfa,
MD5 ( ltrim ( rtrim ( exp_SrcFields1.in_ASSET_ROLE_DT ) ) || ltrim ( rtrim ( exp_SrcFields1.in_ASSET_USE_CD ) ) || ltrim ( rtrim ( exp_SrcFields1.RateDriverclassalfa ) ) ) as v_SRC_MD5,
MD5 ( ltrim ( rtrim ( LKP_PRTY_TO_PRTY_ASSET_CDC_MORTGAGEE.ASSET_ROLE_DTTM ) ) || ltrim ( rtrim ( LKP_PRTY_TO_PRTY_ASSET_CDC_MORTGAGEE.ASSET_USE_CD ) ) || ltrim ( rtrim ( LKP_PRTY_TO_PRTY_ASSET_CDC_MORTGAGEE.DRVR_CLAS_CD ) ) ) as v_TGT_MD5,
CASE WHEN v_TGT_MD5 IS NULL THEN ''I'' ELSE CASE WHEN v_SRC_MD5 = v_TGT_MD5 THEN ''R'' ELSE ''U'' END END as o_SRC_TGT,
exp_SrcFields1.EDW_STRT_DTTM as StartDate,
exp_SrcFields1.EDW_END_DTTm as EndDate,
exp_SrcFields1.in_lienholder_cd as in_lienholder_cd,
exp_SrcFields1.Retired as Retired,
exp_SrcFields1.source_record_id
FROM
exp_SrcFields1
INNER JOIN LKP_PRTY_TO_PRTY_ASSET_CDC_MORTGAGEE ON exp_SrcFields1.source_record_id = LKP_PRTY_TO_PRTY_ASSET_CDC_MORTGAGEE.source_record_id
);


-- Component rtr_CDC1_Insert, Type ROUTER Output Group Insert
CREATE OR REPLACE TEMPORARY TABLE rtr_CDC1_Insert AS (
SELECT
exp_CDC_Check1.in_PRTY_ASSET_ID as in_PRTY_ASSET_ID,
exp_CDC_Check1.in_ASSET_ROLE_CD as in_ASSET_ROLE_CD,
exp_CDC_Check1.in_ASSET_ROLE_DT as in_ASSET_ROLE_DT,
exp_CDC_Check1.in_PRTY_ID as in_PRTY_ID,
exp_CDC_Check1.in_ASSET_USE_CD as in_ASSET_USE_CD,
exp_CDC_Check1.in_PRCS_ID as in_PRCS_ID,
exp_CDC_Check1.lkp_PRTY_ASSET_ID as lkp_PRTY_ASSET_ID,
exp_CDC_Check1.lkp_ASSET_ROLE_CD as lkp_ASSET_ROLE_CD,
exp_CDC_Check1.lkp_PRTY_ID as lkp_PRTY_ID,
exp_CDC_Check1.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_CDC_Check1.o_SRC_TGT as o_SRC_TGT,
exp_CDC_Check1.StartDate as StartDate,
exp_CDC_Check1.EndDate as EndDate,
exp_CDC_Check1.RateDriverclassalfa as RateDriverclassalfa,
exp_CDC_Check1.UPDATETIME as TRANS_STRT_DTTM,
exp_CDC_Check1.lkp_ASSET_ROLE_DT as lkp_ASSET_ROLE_DT,
exp_CDC_Check1.lkp_ASSET_USE_CD as lkp_ASSET_USE_CD,
exp_CDC_Check1.lkp_DRVR_CLAS_CD as lkp_DRVR_CLAS_CD,
exp_CDC_Check1.in_lienholder_cd as in_lienholder_cd,
exp_CDC_Check1.Retired as Retired,
exp_CDC_Check1.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_CDC_Check1.source_record_id
FROM
exp_CDC_Check1
WHERE exp_CDC_Check1.o_SRC_TGT = ''I'' and exp_CDC_Check1.in_PRTY_ASSET_ID <> 9999 and exp_CDC_Check1.in_PRTY_ID <> 9999 OR ( exp_CDC_Check1.Retired = 0 AND exp_CDC_Check1.lkp_EDW_END_DTTM != TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ) OR ( exp_CDC_Check1.o_SRC_TGT = ''U'' and exp_CDC_Check1.in_PRTY_ASSET_ID != 9999 and exp_CDC_Check1.in_PRTY_ID != 9999 AND exp_CDC_Check1.lkp_EDW_END_DTTM = TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ) -- and exp_CDC_Check1.in_PRTY_ID != 9999
);


-- Component rtr_CDC1_Retired, Type ROUTER Output Group Retired
CREATE OR REPLACE TEMPORARY TABLE rtr_CDC1_Retired AS (
SELECT
exp_CDC_Check1.in_PRTY_ASSET_ID as in_PRTY_ASSET_ID,
exp_CDC_Check1.in_ASSET_ROLE_CD as in_ASSET_ROLE_CD,
exp_CDC_Check1.in_ASSET_ROLE_DT as in_ASSET_ROLE_DT,
exp_CDC_Check1.in_PRTY_ID as in_PRTY_ID,
exp_CDC_Check1.in_ASSET_USE_CD as in_ASSET_USE_CD,
exp_CDC_Check1.in_PRCS_ID as in_PRCS_ID,
exp_CDC_Check1.lkp_PRTY_ASSET_ID as lkp_PRTY_ASSET_ID,
exp_CDC_Check1.lkp_ASSET_ROLE_CD as lkp_ASSET_ROLE_CD,
exp_CDC_Check1.lkp_PRTY_ID as lkp_PRTY_ID,
exp_CDC_Check1.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_CDC_Check1.o_SRC_TGT as o_SRC_TGT,
exp_CDC_Check1.StartDate as StartDate,
exp_CDC_Check1.EndDate as EndDate,
exp_CDC_Check1.RateDriverclassalfa as RateDriverclassalfa,
exp_CDC_Check1.UPDATETIME as TRANS_STRT_DTTM,
exp_CDC_Check1.lkp_ASSET_ROLE_DT as lkp_ASSET_ROLE_DT,
exp_CDC_Check1.lkp_ASSET_USE_CD as lkp_ASSET_USE_CD,
exp_CDC_Check1.lkp_DRVR_CLAS_CD as lkp_DRVR_CLAS_CD,
exp_CDC_Check1.in_lienholder_cd as in_lienholder_cd,
exp_CDC_Check1.Retired as Retired,
exp_CDC_Check1.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_CDC_Check1.source_record_id
FROM
exp_CDC_Check1
WHERE exp_CDC_Check1.o_SRC_TGT = ''R'' and exp_CDC_Check1.Retired != 0 and exp_CDC_Check1.lkp_EDW_END_DTTM = TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
);


-- Component upd_INSERT1, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_INSERT1 AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_CDC1_Insert.in_PRTY_ASSET_ID as in_PRTY_ASSET_ID1,
rtr_CDC1_Insert.in_ASSET_ROLE_CD as in_ASSET_ROLE_CD1,
rtr_CDC1_Insert.in_ASSET_ROLE_DT as in_ASSET_ROLE_DT1,
rtr_CDC1_Insert.in_PRTY_ID as in_PRTY_ID1,
rtr_CDC1_Insert.in_ASSET_USE_CD as in_ASSET_USE_CD1,
rtr_CDC1_Insert.in_PRCS_ID as in_PRCS_ID1,
rtr_CDC1_Insert.StartDate as StartDate1,
rtr_CDC1_Insert.EndDate as EndDate1,
rtr_CDC1_Insert.RateDriverclassalfa as RateDriverclassalfa1,
rtr_CDC1_Insert.TRANS_STRT_DTTM as TRANS_STRT_DTTM1,
rtr_CDC1_Insert.in_lienholder_cd as in_lienholder_cd1,
rtr_CDC1_Insert.Retired as Retired1,
rtr_CDC1_Insert.source_record_id,
0 as UPDATE_STRATEGY_ACTION
FROM
rtr_CDC1_Insert
);


-- Component upd_retired1, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_retired1 AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_CDC1_Retired.lkp_PRTY_ASSET_ID as lkp_PRTY_ASSET_ID4,
rtr_CDC1_Retired.lkp_PRTY_ID as lkp_PRTY_ID4,
rtr_CDC1_Retired.lkp_ASSET_ROLE_CD as lkp_ASSET_ROLE_CD4,
rtr_CDC1_Retired.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM4,
rtr_CDC1_Retired.lkp_ASSET_ROLE_DT as lkp_ASSET_ROLE_DT4,
rtr_CDC1_Retired.TRANS_STRT_DTTM as TRANS_STRT_DTTM4,
rtr_CDC1_Retired.source_record_id,
1 as UPDATE_STRATEGY_ACTION
FROM
rtr_CDC1_Retired
);


-- Component exp_RETIRED1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_RETIRED1 AS
(
SELECT
upd_retired1.lkp_PRTY_ASSET_ID4 as lkp_PRTY_ASSET_ID4,
upd_retired1.lkp_PRTY_ID4 as lkp_PRTY_ID4,
upd_retired1.lkp_ASSET_ROLE_CD4 as lkp_ASSET_ROLE_CD4,
upd_retired1.lkp_EDW_STRT_DTTM4 as lkp_EDW_STRT_DTTM4,
upd_retired1.lkp_ASSET_ROLE_DT4 as lkp_ASSET_ROLE_DT4,
upd_retired1.TRANS_STRT_DTTM4 as TRANS_STRT_DTTM41,
CURRENT_TIMESTAMP as o_EDW_END_DTTM,
upd_retired1.source_record_id
FROM
upd_retired1
);


-- Component exp_INSERT1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_INSERT1 AS
(
SELECT
upd_INSERT1.in_PRTY_ASSET_ID1 as in_PRTY_ASSET_ID1,
upd_INSERT1.in_ASSET_ROLE_CD1 as in_ASSET_ROLE_CD1,
upd_INSERT1.in_ASSET_ROLE_DT1 as in_ASSET_ROLE_DT1,
upd_INSERT1.in_PRTY_ID1 as in_PRTY_ID1,
upd_INSERT1.in_ASSET_USE_CD1 as in_ASSET_USE_CD1,
upd_INSERT1.in_PRCS_ID1 as in_PRCS_ID1,
upd_INSERT1.StartDate1 as StartDate1,
upd_INSERT1.RateDriverclassalfa1 as RateDriverclassalfa1,
upd_INSERT1.TRANS_STRT_DTTM1 as TRANS_STRT_DTTM1,
upd_INSERT1.in_lienholder_cd1 as in_lienholder_cd1,
CASE WHEN upd_INSERT1.Retired1 != 0 THEN CURRENT_TIMESTAMP ELSE upd_INSERT1.EndDate1 END as o_EDW_END_DTTM,
to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as TRANS_END_DTTM,
upd_INSERT1.source_record_id
FROM
upd_INSERT1
);


-- Component PRTY_TO_PRTY_ASSET_Retired_Mortgagee, Type TARGET 
MERGE INTO DB_T_PROD_CORE.PRTY_TO_PRTY_ASSET
USING exp_RETIRED1 ON (PRTY_TO_PRTY_ASSET.PRTY_ASSET_ID = exp_RETIRED1.lkp_PRTY_ASSET_ID4 AND PRTY_TO_PRTY_ASSET.ASSET_ROLE_CD = exp_RETIRED1.lkp_ASSET_ROLE_CD4 AND PRTY_TO_PRTY_ASSET.PRTY_ID = exp_RETIRED1.lkp_PRTY_ID4 AND PRTY_TO_PRTY_ASSET.EDW_STRT_DTTM = exp_RETIRED1.lkp_EDW_STRT_DTTM4)
WHEN MATCHED THEN UPDATE
SET
PRTY_ASSET_ID = exp_RETIRED1.lkp_PRTY_ASSET_ID4,
ASSET_ROLE_CD = exp_RETIRED1.lkp_ASSET_ROLE_CD4,
ASSET_ROLE_DTTM = exp_RETIRED1.lkp_ASSET_ROLE_DT4,
PRTY_ID = exp_RETIRED1.lkp_PRTY_ID4,
EDW_STRT_DTTM = exp_RETIRED1.lkp_EDW_STRT_DTTM4,
EDW_END_DTTM = exp_RETIRED1.o_EDW_END_DTTM,
TRANS_END_DTTM = exp_RETIRED1.TRANS_STRT_DTTM41;


-- Component PRTY_TO_PRTY_ASSET_NewInsert_Mortgagee, Type TARGET 
INSERT INTO DB_T_PROD_CORE.PRTY_TO_PRTY_ASSET
(
PRTY_ASSET_ID,
ASSET_ROLE_CD,
ASSET_ROLE_DTTM,
PRTY_ID,
ASSET_USE_CD,
LIENHLDR_POSITN_CD,
DRVR_CLAS_CD,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM,
TRANS_END_DTTM
)
SELECT
exp_INSERT1.in_PRTY_ASSET_ID1 as PRTY_ASSET_ID,
exp_INSERT1.in_ASSET_ROLE_CD1 as ASSET_ROLE_CD,
exp_INSERT1.in_ASSET_ROLE_DT1 as ASSET_ROLE_DTTM,
exp_INSERT1.in_PRTY_ID1 as PRTY_ID,
exp_INSERT1.in_ASSET_USE_CD1 as ASSET_USE_CD,
exp_INSERT1.in_lienholder_cd1 as LIENHLDR_POSITN_CD,
exp_INSERT1.RateDriverclassalfa1 as DRVR_CLAS_CD,
exp_INSERT1.in_PRCS_ID1 as PRCS_ID,
exp_INSERT1.StartDate1 as EDW_STRT_DTTM,
exp_INSERT1.o_EDW_END_DTTM as EDW_END_DTTM,
exp_INSERT1.TRANS_STRT_DTTM1 as TRANS_STRT_DTTM,
exp_INSERT1.TRANS_END_DTTM as TRANS_END_DTTM
FROM
exp_INSERT1;


-- PIPELINE END FOR 3
-- Component PRTY_TO_PRTY_ASSET_NewInsert_Mortgagee, Type Post SQL 
UPDATE  db_t_prod_core.PRTY_TO_PRTY_ASSET   
set 

EDW_END_DTTM=A.lead1,

TRANS_END_DTTM=A.lead2

FROM

(SELECT  distinct PRTY_ASSET_ID,ASSET_ROLE_CD,PRTY_ID,EDW_STRT_DTTM,TRANS_STRT_DTTM,LIENHLDR_POSITN_CD,

max(EDW_STRT_DTTM) over (partition by PRTY_ASSET_ID,ASSET_ROLE_CD,PRTY_ID,LIENHLDR_POSITN_CD ORDER BY EDW_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND'' 

 as lead1,

max(TRANS_STRT_DTTM) over (partition by PRTY_ASSET_ID,ASSET_ROLE_CD,PRTY_ID,LIENHLDR_POSITN_CD ORDER BY EDW_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND'' 

 as lead2

FROM      db_t_prod_core.PRTY_TO_PRTY_ASSET where Asset_Role_Cd =''MRTGEE''

group by PRTY_ASSET_ID,ASSET_ROLE_CD,PRTY_ID,EDW_STRT_DTTM,TRANS_STRT_DTTM,LIENHLDR_POSITN_CD

) a



where PRTY_TO_PRTY_ASSET.EDW_STRT_DTTM = A.EDW_STRT_DTTM

AND PRTY_TO_PRTY_ASSET.PRTY_ASSET_ID=A.PRTY_ASSET_ID

and PRTY_TO_PRTY_ASSET.ASSET_ROLE_CD = A.ASSET_ROLE_CD

and PRTY_TO_PRTY_ASSET.PRTY_ID = A.PRTY_ID

and PRTY_TO_PRTY_ASSET.LIENHLDR_POSITN_CD = A.LIENHLDR_POSITN_CD

and PRTY_TO_PRTY_ASSET.Asset_Role_Cd=''MRTGEE''

AND CAST(EDW_END_DTTM AS DATE)=''9999-12-31''

AND CAST(TRANS_END_DTTM AS DATE)=''9999-12-31''

and lead1 is not null;


END; 
';