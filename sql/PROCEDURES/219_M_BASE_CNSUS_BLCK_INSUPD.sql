-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_CNSUS_BLCK_INSUPD("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' declare
	start_dttm timestamp;
	end_dttm timestamp;
    prcs_id int;
BEGIN 
set start_dttm  = current_timestamp;
set END_DTTM = current_timestamp;
set prcs_id= 1;   

-- Component LKP_CNSUS_BLCK_GRP, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_CNSUS_BLCK_GRP AS
(
SELECT 
CNSUS_BLCK_GRP.CNSUS_BLCK_GRP_ID as CNSUS_BLCK_GRP_ID,CNSUS_BLCK_GRP.CNSUS_TRCT_ID as CNSUS_TRCT_ID,
CNSUS_BLCK_GRP.GEOGRCL_AREA_SBTYPE_CD as GEOGRCL_AREA_SBTYPE_CD, CNSUS_BLCK_GRP.LOCTR_SBTYPE_CD as LOCTR_SBTYPE_CD, CNSUS_BLCK_GRP.GEOGRCL_AREA_STRT_DT as GEOGRCL_AREA_STRT_DT, 
CNSUS_BLCK_GRP.EDW_STRT_DTTM as EDW_STRT_DTTM, CNSUS_BLCK_GRP.EDW_END_DTTM as EDW_END_DTTM, CNSUS_BLCK_GRP.GEOGRCL_AREA_SHRT_NAME as GEOGRCL_AREA_SHRT_NAME,
CNSUS_BLCK_GRP.CNSUS_BLCK_GRP_NUM as CNSUS_BLCK_GRP_NUM
FROM DB_T_PROD_CORE.CNSUS_BLCK_GRP
QUALIFY ROW_NUMBER() OVER(PARTITION BY CNSUS_TRCT_ID,GEOGRCL_AREA_SHRT_NAME,CNSUS_BLCK_GRP_NUM,GEOGRCL_AREA_STRT_DT ORDER BY EDW_END_DTTM desc) = 1/*  */
);


-- Component LKP_CNSUS_TRCT, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_CNSUS_TRCT AS
(
SELECT 
CNSUS_TRCT.CNSUS_TRCT_ID as CNSUS_TRCT_ID,
CNSUS_TRCT.CNTY_ID as CNTY_ID, CNSUS_TRCT.GEOGRCL_AREA_SBTYPE_CD as GEOGRCL_AREA_SBTYPE_CD, CNSUS_TRCT.LOCTR_SBTYPE_CD as LOCTR_SBTYPE_CD, CNSUS_TRCT.GEOGRCL_AREA_STRT_DT as GEOGRCL_AREA_STRT_DT, 
CNSUS_TRCT.EDW_STRT_DTTM as EDW_STRT_DTTM, CNSUS_TRCT.EDW_END_DTTM as EDW_END_DTTM, CNSUS_TRCT.GEOGRCL_AREA_SHRT_NAME as GEOGRCL_AREA_SHRT_NAME,
CNSUS_TRCT.CNSUS_TRCT_NUM as CNSUS_TRCT_NUM 
 FROM DB_T_PROD_CORE.CNSUS_TRCT
QUALIFY ROW_NUMBER() OVER(PARTITION BY CNTY_ID,GEOGRCL_AREA_SHRT_NAME,CNSUS_TRCT_NUM ORDER BY EDW_END_DTTM desc) = 1/*  */
);


-- Component LKP_COUNTY, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_COUNTY AS
(
SELECT CNTY.CNTY_ID as CNTY_ID, CNTY.TERR_ID as TERR_ID, CNTY.GEOGRCL_AREA_SBTYPE_CD as GEOGRCL_AREA_SBTYPE_CD

, ltrim(rtrim(GEOGRCL_AREA_SHRT_NAME)) as GEOGRCL_AREA_SHRT_NAME,LOCTR_SBTYPE_CD as LOCTR_SBTYPE_CD

,GEOGRCL_AREA_STRT_DTTM as GEOGRCL_AREA_STRT_DT,EDW_END_DTTM as EDW_END_DTTM 

FROM DB_T_PROD_CORE.CNTY



QUALIFY ROW_NUMBER() OVER(PARTITION BY TERR_ID,GEOGRCL_AREA_SHRT_NAME ORDER BY EDW_END_DTTM desc) = 1
);


-- Component LKP_CTRY, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_CTRY AS
(
SELECT CTRY.CTRY_ID as CTRY_ID, CTRY.GEOGRCL_AREA_NAME as GEOGRCL_AREA_NAME, CTRY.GEOGRCL_AREA_DESC as GEOGRCL_AREA_DESC, CTRY.EDW_STRT_DTTM as EDW_STRT_DTTM, CTRY.EDW_END_DTTM as EDW_END_DTTM, CTRY.GEOGRCL_AREA_SHRT_NAME as GEOGRCL_AREA_SHRT_NAME 

FROM DB_T_PROD_CORE.CTRY

WHERE CAST(CTRY.EDW_END_DTTM AS DATE)=CAST(''9999-12-31'' AS DATE)
);


-- Component LKP_POSTL_CD, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_POSTL_CD AS
(
SELECT POSTL_CD.POSTL_CD_ID as POSTL_CD_ID, POSTL_CD.CTRY_ID as CTRY_ID, POSTL_CD.POSTL_CD_NUM as POSTL_CD_NUM 
FROM DB_T_PROD_CORE.POSTL_CD 
QUALIFY ROW_NUMBER() OVER(PARTITION BY CTRY_ID, POSTL_CD_NUM  
ORDER BY EDW_END_DTTM desc) = 1
);


-- Component LKP_TERADATA_ETL_REF_XLAT_GEOGRCL_AREA_SBTYPE, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_GEOGRCL_AREA_SBTYPE AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''GEOGRCL_AREA_SBTYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_LOCTR_SBTYPE, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_LOCTR_SBTYPE AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''LOCTR_SBTYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERR, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERR AS
(
SELECT TERR.TERR_ID as TERR_ID, TERR.GEOGRCL_AREA_NAME as GEOGRCL_AREA_NAME, TERR.GEOGRCL_AREA_DESC as GEOGRCL_AREA_DESC, TERR.GEOGRCL_AREA_STRT_DTTM as GEOGRCL_AREA_STRT_DTTM, TERR.GEOGRCL_AREA_END_DTTM as GEOGRCL_AREA_END_DTTM, TERR.LOCTR_SBTYPE_CD as LOCTR_SBTYPE_CD, TERR.GEOGRCL_AREA_SBTYPE_CD as GEOGRCL_AREA_SBTYPE_CD, TERR.EDW_STRT_DTTM as EDW_STRT_DTTM, TERR.EDW_END_DTTM as EDW_END_DTTM, TERR.CTRY_ID as CTRY_ID, TERR.GEOGRCL_AREA_SHRT_NAME as GEOGRCL_AREA_SHRT_NAME FROM DB_T_PROD_CORE.TERR

WHERE CAST(TERR.EDW_END_DTTM AS DATE)=CAST(''9999-12-31'' AS DATE)
);


-- PIPELINE START FOR 1

-- Component sq_pc_address, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_pc_address AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as state_TYPECODE,
$2 as ctry_TYPECODE,
$3 as County,
$4 as CreateTime,
$5 as Retired,
$6 as TerritoryFIPSCode_alfa,
$7 as PostalCode,
$8 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select state_TYPECODE,case when (ctry_TYPECODE is null) then ''US'' else ctry_TYPECODE end ctry_TYPECD,UPPER(county),createtime,cast (0 as bigint) Retired,TerritoryFIPSCode_alfa,PostalCode

from (

SELECT  state_TYPECODE as state_TYPECODE, ctry_TYPECODE as ctry_TYPECODE, pc_address.county as county

,pc_address.createtime,CASE WHEN LENGTH(TRIM(TerritoryFIPSCode_alfa))=15 THEN TerritoryFIPSCode_alfa ELSE NULL END AS TerritoryFIPSCode_alfa

,PostalCode as PostalCode

FROM  

(SELECT	

pc_address.CreateTime_stg as createtime,		

		pc_address.County_stg as county,

		pc_address.PostalCode_stg as PostalCode,

		pc_address.State_stg as State,

		pc_address.Country_stg as Country,

		TerritoryFIPSCode_alfa_stg as TerritoryFIPSCode_alfa 

FROM	

 DB_T_PROD_STAG.pc_address

WHERE	pc_address.UpdateTime_stg > (:start_dttm)

	and	pc_address.UpdateTime_stg <= (:end_dttm) 

	)  pc_address left join (SELECT pctl_country.ID_stg as ID_stg ,pctl_country.TYPECODE_stg as ctry_TYPECODE FROM DB_T_PROD_STAG.pctl_country) pctl_country on pctl_country.ID_stg=pc_address.Country 

	join (SELECT pctl_state.ID_stg  as ID_stg,pctl_state.TYPECODE_stg as state_TYPECODE FROM DB_T_PROD_STAG.pctl_state) pctl_state on State =pctl_state.ID_stg 

where pc_address.county is not null AND pc_address.TerritoryFIPSCode_alfa is not null

UNION

SELECT  state_TYPECODE as state_TYPECODE, ctry_TYPECODE as ctry_TYPECODE,

		pc_policylocation.CountyInternal

        AS County,pc_policylocation.createtime

		,CASE WHEN LENGTH(TRIM(TerritoryFIPSCode_alfaInternal))=15 THEN TerritoryFIPSCode_alfaInternal ELSE NULL

		END AS TerritoryFIPSCode_alfa

		,SUBSTR(PostalCodeInternal,1,CASE	WHEN POSITION(''-'',PostalCodeInternal) = 0	THEN LENGTH(PostalCodeInternal) ELSE POSITION(''-'',PostalCodeInternal)-1

			END) as PostalCode

FROM	

( SELECT 

 pc_policylocation.CountyInternal_stg as CountyInternal,

 pc_policylocation.CountryInternal_stg as CountryInternal,

 pc_policylocation.CreateTime_stg as CreateTime  ,

 pc_policylocation.PostalCodeInternal_stg as PostalCodeInternal,

 pc_policylocation.StateInternal_stg as StateInternal,

 pc_policylocation.TerritoryFIPSCode_alfainternal_stg as TerritoryFIPSCode_alfainternal

FROM

 DB_T_PROD_STAG.pc_policylocation

where 

pc_policylocation.UpdateTime_stg > (:start_dttm)

and pc_policylocation.UpdateTime_stg <= (:end_dttm) 

) pc_policylocation

 join (SELECT pctl_state.ID_stg  as ID_stg,pctl_state.TYPECODE_stg as state_TYPECODE FROM DB_T_PROD_STAG.pctl_state) pctl_state on	 pc_policylocation.StateInternal=pctl_state.ID_stg 

left join (SELECT pctl_country.ID_stg as ID_stg ,pctl_country.TYPECODE_stg as ctry_TYPECODE FROM DB_T_PROD_STAG.pctl_country) pctl_country 	on	pctl_country.ID_stg=pc_policylocation.CountryInternal

where	pc_policylocation.CountyInternal is not null and TerritoryFIPSCode_alfaInternal is not null

) a  where TerritoryFIPSCode_alfa is not null

QUALIFY ROW_NUMBER() OVER (PARTITION BY  state_typecode,ctry_typecd,upper(county),RETIRED,TerritoryFIPSCode_alfa,postalcode

ORDER BY createtime DESC)=1
) SRC
)
);


-- Component exp_pass_through_id, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_through_id AS
(
SELECT
sq_pc_address.ctry_TYPECODE as ctry_TYPECODE,
sq_pc_address.state_TYPECODE as state_TYPECODE,
sq_pc_address.County as County,
LTRIM ( RTRIM ( CASE WHEN sq_pc_address.TerritoryFIPSCode_alfa IS NULL THEN sq_pc_address.TerritoryFIPSCode_alfa ELSE SUBSTR ( sq_pc_address.TerritoryFIPSCode_alfa , 12 , 4 ) END ) ) as v_TerritoryFIPSCode_alfa_blck,
LTRIM ( RTRIM ( CASE WHEN sq_pc_address.TerritoryFIPSCode_alfa IS NULL THEN sq_pc_address.TerritoryFIPSCode_alfa ELSE substr ( sq_pc_address.TerritoryFIPSCode_alfa , 6 , 6 ) END ) ) as v_TerritoryFIPSCode_alfa_tct,
LKP_1.CTRY_ID /* replaced lookup LKP_CTRY */ as v_ctry_id,
LKP_2.TERR_ID /* replaced lookup LKP_TERR */ as v_terr_id,
LKP_3.CNTY_ID /* replaced lookup LKP_COUNTY */ as v_cnty_id,
LKP_4.POSTL_CD_ID /* replaced lookup LKP_POSTL_CD */ as v_POSTL_CD_ID,
LKP_5.CNSUS_TRCT_ID /* replaced lookup LKP_CNSUS_TRCT */ as v_CNSUS_TRCT_ID,
LKP_6.CNSUS_BLCK_GRP_ID /* replaced lookup LKP_CNSUS_BLCK_GRP */ as v_CNSUS_BLCK_GRP_ID,
CASE WHEN v_CNSUS_BLCK_GRP_ID IS NULL THEN 9999 ELSE v_CNSUS_BLCK_GRP_ID END as o_CNSUS_BLCK_GRP_ID,
CASE WHEN v_POSTL_CD_ID IS NULL THEN 9999 ELSE v_POSTL_CD_ID END as O_postl_cd_id,
sq_pc_address.CreateTime as CreateTime,
sq_pc_address.Retired as Retired,
LTRIM ( RTRIM ( sq_pc_address.TerritoryFIPSCode_alfa ) ) as o_TerritoryFIPSCode_alfa,
sq_pc_address.source_record_id,
row_number() over (partition by sq_pc_address.source_record_id order by sq_pc_address.source_record_id) as RNK
FROM
sq_pc_address
LEFT JOIN LKP_CTRY LKP_1 ON LKP_1.GEOGRCL_AREA_SHRT_NAME = sq_pc_address.ctry_TYPECODE
LEFT JOIN LKP_TERR LKP_2 ON LKP_2.CTRY_ID = v_ctry_id AND LKP_2.GEOGRCL_AREA_SHRT_NAME = sq_pc_address.state_TYPECODE
LEFT JOIN LKP_COUNTY LKP_3 ON LKP_3.TERR_ID = v_terr_id AND LKP_3.GEOGRCL_AREA_SHRT_NAME = ltrim ( rtrim ( sq_pc_address.County ) )
LEFT JOIN LKP_POSTL_CD LKP_4 ON LKP_4.CTRY_ID = v_ctry_id AND LKP_4.POSTL_CD_NUM = sq_pc_address.PostalCode
LEFT JOIN LKP_CNSUS_TRCT LKP_5 ON LKP_5.CNTY_ID = v_cnty_id AND LKP_5.GEOGRCL_AREA_SHRT_NAME = ltrim ( rtrim ( sq_pc_address.County ) ) AND LKP_5.CNSUS_TRCT_NUM = v_TerritoryFIPSCode_alfa_tct
LEFT JOIN LKP_CNSUS_BLCK_GRP LKP_6 ON LKP_6.CNSUS_TRCT_ID = v_CNSUS_TRCT_ID AND LKP_6.CNSUS_BLCK_GRP_NUM = v_TerritoryFIPSCode_alfa_blck AND LKP_6.GEOGRCL_AREA_SHRT_NAME = ltrim ( rtrim ( sq_pc_address.County ) )
QUALIFY RNK = 1
);


-- Component exp_PASS_THROUGH_POSTL_CD_ID, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_PASS_THROUGH_POSTL_CD_ID AS
(
SELECT
exp_pass_through_id.ctry_TYPECODE as ctry_TYPECODE,
exp_pass_through_id.state_TYPECODE as state_TYPECODE,
exp_pass_through_id.County as County,
exp_pass_through_id.o_CNSUS_BLCK_GRP_ID as o_CNSUS_BLCK_GRP_ID,
exp_pass_through_id.O_postl_cd_id as O_postl_cd_id,
exp_pass_through_id.CreateTime as o_CreateTime,
exp_pass_through_id.Retired as Retired,
exp_pass_through_id.o_TerritoryFIPSCode_alfa as o_TerritoryFIPSCode_alfa,
exp_pass_through_id.source_record_id
FROM
exp_pass_through_id
);


-- Component LKP_CNSUS_BLCK, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_CNSUS_BLCK AS
(
SELECT
LKP.CNSUS_BLCK_ID,
LKP.CNSUS_BLCK_GRP_ID,
LKP.POSTL_CD_ID,
LKP.GEOGRCL_AREA_SBTYPE_CD,
LKP.LOCTR_SBTYPE_CD,
exp_PASS_THROUGH_POSTL_CD_ID.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_PASS_THROUGH_POSTL_CD_ID.source_record_id ORDER BY LKP.CNSUS_BLCK_ID desc,LKP.CNSUS_BLCK_GRP_ID desc,LKP.POSTL_CD_ID desc,LKP.GEOGRCL_AREA_SBTYPE_CD desc,LKP.LOCTR_SBTYPE_CD desc) RNK
FROM
exp_PASS_THROUGH_POSTL_CD_ID
LEFT JOIN (
SELECT 
CNSUS_BLCK.CNSUS_BLCK_ID as CNSUS_BLCK_ID,CNSUS_BLCK.CNSUS_BLCK_GRP_ID as CNSUS_BLCK_GRP_ID,
CNSUS_BLCK.POSTL_CD_ID as POSTL_CD_ID,
CNSUS_BLCK.GEOGRCL_AREA_SBTYPE_CD as GEOGRCL_AREA_SBTYPE_CD, CNSUS_BLCK.LOCTR_SBTYPE_CD as LOCTR_SBTYPE_CD,CNSUS_BLCK.GEOGRCL_AREA_SHRT_NAME as GEOGRCL_AREA_SHRT_NAME,
CNSUS_BLCK.FIPS_NUM as FIPS_NUM
FROM DB_T_PROD_CORE.CNSUS_BLCK
QUALIFY ROW_NUMBER() OVER(PARTITION BY CNSUS_BLCK_GRP_ID,POSTL_CD_ID,GEOGRCL_AREA_SHRT_NAME,FIPS_NUM,GEOGRCL_AREA_STRT_DT ORDER BY EDW_END_DTTM desc) = 1/*  */
) LKP ON LKP.CNSUS_BLCK_GRP_ID = exp_PASS_THROUGH_POSTL_CD_ID.o_CNSUS_BLCK_GRP_ID AND LKP.POSTL_CD_ID = exp_PASS_THROUGH_POSTL_CD_ID.O_postl_cd_id AND LKP.GEOGRCL_AREA_SHRT_NAME = exp_PASS_THROUGH_POSTL_CD_ID.County AND LKP.FIPS_NUM = exp_PASS_THROUGH_POSTL_CD_ID.o_TerritoryFIPSCode_alfa
QUALIFY RNK = 1
);


-- Component exp_SET_INS_UPD_FLAG, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_SET_INS_UPD_FLAG AS
(
SELECT
exp_PASS_THROUGH_POSTL_CD_ID.County as County,
''LOCTR_SBTYPE3'' as v_loctr_sbtype_val,
''GEOGRCL_AREA_SBTYPE3'' as v_geogrcl_sbtype_val,
LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_LOCTR_SBTYPE */ as v_loctr_sbtype,
LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_GEOGRCL_AREA_SBTYPE */ as v_geogrcl_sbtype,
v_loctr_sbtype as o_loctr_sbtype,
v_geogrcl_sbtype as o_geogrcl_sbtype,
:PRCS_ID as o_process_id,
exp_PASS_THROUGH_POSTL_CD_ID.o_CreateTime as CreateTime,
exp_PASS_THROUGH_POSTL_CD_ID.o_TerritoryFIPSCode_alfa as TerritoryFIPSCode_alfa,
MD5 ( LKP_CNSUS_BLCK.GEOGRCL_AREA_SBTYPE_CD || LKP_CNSUS_BLCK.LOCTR_SBTYPE_CD ) as lkp_checksum,
MD5 ( v_geogrcl_sbtype || v_loctr_sbtype ) as in_checksum,
CASE WHEN LKP_CNSUS_BLCK.CNSUS_BLCK_ID IS NULL THEN ''I'' ELSE ( CASE WHEN lkp_checksum <> in_checksum THEN ''U'' ELSE ''R'' END ) END as o_upd_or_ins,
CASE WHEN LKP_CNSUS_BLCK.CNSUS_BLCK_ID IS NULL THEN ''I'' ELSE ( CASE WHEN lkp_checksum <> in_checksum THEN ''U'' ELSE ''R'' END ) END as v_upd_or_ins,
CURRENT_TIMESTAMP as EDW_STRT_DTTM,
TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as EDW_END_DTTM,
CASE WHEN v_upd_or_ins = ''U'' THEN LKP_CNSUS_BLCK.CNSUS_BLCK_GRP_ID ELSE exp_PASS_THROUGH_POSTL_CD_ID.o_CNSUS_BLCK_GRP_ID END as o_CNSUS_BLCK_GRP_ID,
CASE WHEN v_upd_or_ins = ''U'' THEN LKP_CNSUS_BLCK.POSTL_CD_ID ELSE exp_PASS_THROUGH_POSTL_CD_ID.O_postl_cd_id END as o_POSTL_CD_ID,
CASE WHEN v_upd_or_ins = ''U'' THEN LKP_CNSUS_BLCK.CNSUS_BLCK_ID ELSE NULL END as lkp_CNSUS_BLCK_ID1,
exp_PASS_THROUGH_POSTL_CD_ID.source_record_id,
row_number() over (partition by exp_PASS_THROUGH_POSTL_CD_ID.source_record_id order by exp_PASS_THROUGH_POSTL_CD_ID.source_record_id) as RNK
FROM
exp_PASS_THROUGH_POSTL_CD_ID
INNER JOIN LKP_CNSUS_BLCK ON exp_PASS_THROUGH_POSTL_CD_ID.source_record_id = LKP_CNSUS_BLCK.source_record_id
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_LOCTR_SBTYPE LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = v_loctr_sbtype_val
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_GEOGRCL_AREA_SBTYPE LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = v_geogrcl_sbtype_val
QUALIFY RNK = 1
);


-- Component rtr_update_insert_CNSUS_INS, Type ROUTER Output Group INS
CREATE OR REPLACE TEMPORARY TABLE rtr_update_insert_CNSUS_INS AS
(
SELECT
exp_SET_INS_UPD_FLAG.County as Short_Name,
exp_SET_INS_UPD_FLAG.County as Name,
exp_SET_INS_UPD_FLAG.o_upd_or_ins as UPD_OR_INS,
exp_SET_INS_UPD_FLAG.o_process_id as o_process_id,
exp_SET_INS_UPD_FLAG.o_loctr_sbtype as o_loctr_sbtype,
exp_SET_INS_UPD_FLAG.o_geogrcl_sbtype as o_geogrcl_sbtype,
exp_SET_INS_UPD_FLAG.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_SET_INS_UPD_FLAG.EDW_END_DTTM as EDW_END_DTTM,
exp_SET_INS_UPD_FLAG.CreateTime as CreateTime,
exp_SET_INS_UPD_FLAG.TerritoryFIPSCode_alfa as o_TerritoryFIPSCode_alfa,
exp_SET_INS_UPD_FLAG.o_CNSUS_BLCK_GRP_ID as CNSUS_BLCK_GRP_ID,
exp_SET_INS_UPD_FLAG.o_POSTL_CD_ID as POSTL_CD_ID,
exp_SET_INS_UPD_FLAG.lkp_CNSUS_BLCK_ID1 as lkp_CNSUS_BLCK_ID,
exp_SET_INS_UPD_FLAG.source_record_id
FROM
exp_SET_INS_UPD_FLAG
WHERE ( exp_SET_INS_UPD_FLAG.o_upd_or_ins = ''I'' ) or ( exp_SET_INS_UPD_FLAG.o_upd_or_ins = ''U'' ) 
-- OR ( exp_SET_INS_UPD_FLAG.o_upd_or_ins = ''U'' ) 
-- exp_SET_INS_UPD_FLAG.o_upd_or_ins = ''U'' AND lkp_EDW_END_DTTM = TO_timestamp ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
);


-- Component upd_insert_update_CNSUS, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_insert_update_CNSUS AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_update_insert_CNSUS_INS.Short_Name as Short_Name1,
rtr_update_insert_CNSUS_INS.Name as Name1,
rtr_update_insert_CNSUS_INS.o_process_id as o_process_id1,
rtr_update_insert_CNSUS_INS.o_loctr_sbtype as o_loctr_sbtype1,
rtr_update_insert_CNSUS_INS.o_geogrcl_sbtype as o_geogrcl_sbtype1,
rtr_update_insert_CNSUS_INS.EDW_STRT_DTTM as EDW_STRT_DTTM1,
rtr_update_insert_CNSUS_INS.EDW_END_DTTM as EDW_END_DTTM1,
rtr_update_insert_CNSUS_INS.CreateTime as CreateTime,
rtr_update_insert_CNSUS_INS.o_TerritoryFIPSCode_alfa as o_TerritoryFIPSCode_alfa1,
rtr_update_insert_CNSUS_INS.CNSUS_BLCK_GRP_ID as CNSUS_BLCK_GRP_ID,
rtr_update_insert_CNSUS_INS.POSTL_CD_ID as POSTL_CD_ID1,
rtr_update_insert_CNSUS_INS.lkp_CNSUS_BLCK_ID as lkp_CNSUS_BLCK_ID1,
0 as UPDATE_STRATEGY_ACTION,
rtr_update_insert_CNSUS_INS.SOURCE_RECORD_ID
FROM
rtr_update_insert_CNSUS_INS
);


-- Component exp_pass_to_target_insert_pc111, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_target_insert_pc111 AS
(
SELECT
upd_insert_update_CNSUS.Short_Name1 as Short_Name1,
upd_insert_update_CNSUS.Name1 as Name1,
upd_insert_update_CNSUS.o_process_id1 as o_process_id1,
upd_insert_update_CNSUS.o_loctr_sbtype1 as o_loctr_sbtype1,
upd_insert_update_CNSUS.o_geogrcl_sbtype1 as o_geogrcl_sbtype1,
upd_insert_update_CNSUS.EDW_STRT_DTTM1 as EDW_STRT_DTTM1,
upd_insert_update_CNSUS.EDW_END_DTTM1 as EDW_END_DTTM1,
upd_insert_update_CNSUS.CreateTime as CreateTime,
CASE WHEN upd_insert_update_CNSUS.lkp_CNSUS_BLCK_ID1 IS NULL THEN row_number() over (order by 1) ELSE upd_insert_update_CNSUS.lkp_CNSUS_BLCK_ID1 END as o_NEXTVAL,
upd_insert_update_CNSUS.o_TerritoryFIPSCode_alfa1 as o_TerritoryFIPSCode_alfa1,
upd_insert_update_CNSUS.CNSUS_BLCK_GRP_ID as CNSUS_BLCK_GRP__ID,
upd_insert_update_CNSUS.POSTL_CD_ID1 as POSTL_CD_ID1,
upd_insert_update_CNSUS.source_record_id
FROM
upd_insert_update_CNSUS
);


-- Component CNSUS_BLCK, Type TARGET 
INSERT INTO DB_T_PROD_CORE.CNSUS_BLCK
(
CNSUS_BLCK_ID,
CNSUS_BLCK_GRP_ID,
POSTL_CD_ID,
GEOGRCL_AREA_SBTYPE_CD,
GEOGRCL_AREA_SHRT_NAME,
GEOGRCL_AREA_NAME,
GEOGRCL_AREA_STRT_DT,
GEOGRCL_AREA_END_DT,
LOCTR_SBTYPE_CD,
FIPS_NUM,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM
)
SELECT
exp_pass_to_target_insert_pc111.o_NEXTVAL as CNSUS_BLCK_ID,
exp_pass_to_target_insert_pc111.CNSUS_BLCK_GRP__ID as CNSUS_BLCK_GRP_ID,
exp_pass_to_target_insert_pc111.POSTL_CD_ID1 as POSTL_CD_ID,
exp_pass_to_target_insert_pc111.o_geogrcl_sbtype1 as GEOGRCL_AREA_SBTYPE_CD,
exp_pass_to_target_insert_pc111.Short_Name1 as GEOGRCL_AREA_SHRT_NAME,
exp_pass_to_target_insert_pc111.Name1 as GEOGRCL_AREA_NAME,
exp_pass_to_target_insert_pc111.CreateTime as GEOGRCL_AREA_STRT_DT,
exp_pass_to_target_insert_pc111.EDW_END_DTTM1 as GEOGRCL_AREA_END_DT,
exp_pass_to_target_insert_pc111.o_loctr_sbtype1 as LOCTR_SBTYPE_CD,
exp_pass_to_target_insert_pc111.o_TerritoryFIPSCode_alfa1 as FIPS_NUM,
exp_pass_to_target_insert_pc111.o_process_id1 as PRCS_ID,
exp_pass_to_target_insert_pc111.EDW_STRT_DTTM1 as EDW_STRT_DTTM,
exp_pass_to_target_insert_pc111.EDW_END_DTTM1 as EDW_END_DTTM
FROM
exp_pass_to_target_insert_pc111;


-- PIPELINE END FOR 1

-- PIPELINE START FOR 2

-- Component sq_pc_address1, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_pc_address1 AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as state_TYPECODE,
$2 as ctry_TYPECODE,
$3 as County,
$4 as CreateTime,
$5 as Retired,
$6 as TerritoryFIPSCode_alfa,
$7 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select state_TYPECODE,case when (ctry_TYPECODE is null) then ''US'' else ctry_TYPECODE end ctry_TYPECD
--,county(NOT CASESPECIFIC)
,max(createtime),cast (0 as bigint) Retired,TerritoryFIPSCode_alfa,PostalCode

from (

SELECT   state_TYPECODE as state_TYPECODE, ctry_TYPECODE as ctry_TYPECODE, pc_address.county,pc_address.createtime,pc_address.TerritoryFIPSCode_alfa,PostalCode

FROM  (SELECT	

pc_address.CreateTime_stg as createtime,		

		pc_address.County_stg as county,

		pc_address.PostalCode_stg as PostalCode,

		pc_address.State_stg as State,

		pc_address.Country_stg as Country,

		TerritoryFIPSCode_alfa_stg as TerritoryFIPSCode_alfa 

FROM	

 DB_T_PROD_STAG.pc_address

WHERE	pc_address.UpdateTime_stg > (:start_dttm)

	and	pc_address.UpdateTime_stg <= (:end_dttm) 

	) pc_address left join (SELECT pctl_country.ID_stg as ID_stg ,pctl_country.TYPECODE_stg as ctry_TYPECODE FROM DB_T_PROD_STAG.pctl_country ) pctl_country on pctl_country.ID_stg=pc_address.Country 

	join (SELECT pctl_state.ID_stg  as ID_stg,pctl_state.TYPECODE_stg as state_TYPECODE FROM DB_T_PROD_STAG.pctl_state) pctl_state on pc_address.State=pctl_state.ID_stg 

where pc_address.county is not null AND PostalCode is not null

UNION

SELECT	 state_TYPECODE as state_TYPECODE, ctry_TYPECODE as ctry_TYPECODE,

		CountyInternal
        --(NOT CASESPECIFIC) 
        AS County,createtime,TerritoryFIPSCode_alfainternal,SUBSTR(PostalCodeInternal,1, 

CASE	WHEN POSITION(''-'',PostalCodeInternal) = 0

			THEN LENGTH(PostalCodeInternal)

		ELSE POSITION(''-'',PostalCodeInternal)-1

			END) as PostalCode

FROM	 ( SELECT 

 pc_policylocation.CountyInternal_stg as CountyInternal,

 pc_policylocation.CountryInternal_stg as CountryInternal,

 pc_policylocation.CreateTime_stg as CreateTime  ,

 pc_policylocation.PostalCodeInternal_stg as PostalCodeInternal,

 pc_policylocation.StateInternal_stg as StateInternal,

 pc_policylocation.TerritoryFIPSCode_alfainternal_stg as TerritoryFIPSCode_alfainternal

FROM

 DB_T_PROD_STAG.pc_policylocation

where 

pc_policylocation.UpdateTime_stg > (:start_dttm)

and pc_policylocation.UpdateTime_stg <= (:end_dttm) 

) pc_policylocation join (SELECT pctl_state.ID_stg as ID_stg,pctl_state.TYPECODE_stg as  state_TYPECODE FROM DB_T_PROD_STAG.pctl_state) pctl_state 

	on	 pc_policylocation.StateInternal=pctl_state.ID_stg 

	left join (SELECT pctl_country.ID_stg as ID_stg ,pctl_country.TYPECODE_stg as ctry_TYPECODE FROM DB_T_PROD_STAG.pctl_country ) pctl_country 

	on	pctl_country.ID_stg=pc_policylocation.CountryInternal

where	pc_policylocation.CountyInternal is not null and PostalCodeInternal is not null

) a where 1=2

group by state_TYPECODE,ctry_TYPECD
--,county(NOT CASESPECIFIC)
,TerritoryFIPSCode_alfa,PostalCode
) SRC
)
);


-- Component exp_pass_through_id1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_through_id1 AS
(
SELECT
sq_pc_address1.TerritoryFIPSCode_alfa as o_TerritoryFIPSCode_alfa,
sq_pc_address1.source_record_id
FROM
sq_pc_address1
);


-- Component CNSUS_BLCK1, Type TARGET 
INSERT INTO DB_T_PROD_CORE.CNSUS_BLCK
(
FIPS_NUM
)
SELECT
exp_pass_through_id1.o_TerritoryFIPSCode_alfa as FIPS_NUM
FROM
exp_pass_through_id1;


-- PIPELINE END FOR 2
-- Component CNSUS_BLCK1, Type Post SQL 
UPDATE  DB_T_PROD_CORE.CNSUS_BLCK  FROM  

(

SELECT	distinct  CNSUS_BLCK_ID,CNSUS_BLCK_GRP_ID,POSTL_CD_ID,GEOGRCL_AREA_SHRT_NAME,FIPS_NUM,GEOGRCL_AREA_STRT_DT,EDW_STRT_DTTM,

max(EDW_STRT_DTTM) over (partition by   CNSUS_BLCK_GRP_ID,POSTL_CD_ID,GEOGRCL_AREA_SHRT_NAME,FIPS_NUM ORDER BY EDW_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND'' 

 as LEAD1

FROM 

DB_T_PROD_CORE.CNSUS_BLCK  

)  A

set EDW_END_DTTM=A.LEAD1

where  CNSUS_BLCK.CNSUS_BLCK_ID=A.CNSUS_BLCK_ID

AND CNSUS_BLCK.CNSUS_BLCK_GRP_ID=A.CNSUS_BLCK_GRP_ID

AND CNSUS_BLCK.POSTL_CD_ID=A.POSTL_CD_ID

and CNSUS_BLCK.GEOGRCL_AREA_SHRT_NAME=A.GEOGRCL_AREA_SHRT_NAME

and CNSUS_BLCK.FIPS_NUM=A.FIPS_NUM

and CNSUS_BLCK.EDW_STRT_DTTM = A.EDW_STRT_DTTM

and CAST(CNSUS_BLCK.EDW_END_DTTM AS DATE)=''9999-12-31''

and LEAD1  is not null;


END; ';