-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_POSTL_CD_INSUPD("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' DECLARE 

start_dttm TIMESTAMP;
end_dttm TIMESTAMP;
PRCS_ID INTEGER;
P_DEFAULT_STR_CD char;
BEGIN 
start_dttm := CURRENT_TIMESTAMP();
end_dttm := CURRENT_TIMESTAMP();
PRCS_ID := 1;    

-- Component LKP_CTRY, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_CTRY AS
(
SELECT CTRY.CTRY_ID as CTRY_ID, CTRY.GEOGRCL_AREA_NAME as GEOGRCL_AREA_NAME, CTRY.GEOGRCL_AREA_DESC as GEOGRCL_AREA_DESC, CTRY.EDW_STRT_DTTM as EDW_STRT_DTTM, CTRY.EDW_END_DTTM as EDW_END_DTTM, CTRY.GEOGRCL_AREA_SHRT_NAME as GEOGRCL_AREA_SHRT_NAME 

FROM DB_T_PROD_CORE.CTRY

WHERE CAST(CTRY.EDW_END_DTTM AS DATE)=CAST(''9999-12-31'' AS DATE)
);


-- Component LKP_POSTALCODE, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_POSTALCODE AS
(
SELECT POSTL_CD.POSTL_CD_ID as POSTL_CD_ID, POSTL_CD.CTRY_ID as CTRY_ID, POSTL_CD.POSTL_CD_NUM as POSTL_CD_NUM 

FROM DB_T_PROD_CORE.POSTL_CD 

WHERE CAST(POSTL_CD.EDW_END_DTTM AS DATE)=CAST(''9999-12-31'' AS DATE)
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


-- Component sq_cc_address, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_cc_address AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as ctry_typecode,
$2 as PostalCode,
$3 as Strt_dt,
$4 as End_dt,
$5 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT case when ctry_TYPECODE is null then ''US'' else ctry_TYPECODE end ctry_TYPECODE

, PostalCode, max(STRT_DT), END_DT

from (

SELECT bctl_country.TYPECODE_stg as ctry_TYPECODE,bc_address.PostalCode_stg as PostalCode, 

bc_address.createtime_stg as STRT_DT, cast(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6)) as END_DT

FROM (SELECT  bc_address.CreateTime_stg,bc_address.PostalCode_stg,bc_address.Country_stg

FROM DB_T_PROD_STAG.bc_address

WHERE bc_address.UpdateTime_stg>(:start_dttm) AND bc_address.UpdateTime_stg <= (:end_dttm)

) bc_address left  join (select TYPECODE_stg,ID_stg from DB_T_PROD_STAG.bctl_country ) bctl_country on bctl_country.ID_stg=bc_address.Country_stg WHERE bc_address.PostalCode_stg is not null

UNION

SELECT cctl_country.TYPECODE_stg as ctry_TYPECODE,cc_address.PostalCode_stg as PostalCode, 

cc_address.createtime_stg as STRT_DT, cast(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6)) as END_DT

FROM (SELECT  cc_address.CreateTime_stg,

cc_address.PostalCode_stg,Country_stg

FROM DB_T_PROD_STAG.Cc_address WHERE Cc_address.UpdateTime_stg>(:start_dttm) AND Cc_address.UpdateTime_stg <= (:end_dttm)) Cc_address  

left join(select cctl_country.TYPECODE_stg,cctl_country.ID_stg from DB_T_PROD_STAG.cctl_country) cctl_country on cctl_country.ID_stg=cc_address.Country_stg WHERE cc_address.PostalCode_stg is not null

UNION

SELECT   

''US'' as ctry_TYPECODE ,

PostalCode_stg as PostalCode,

createtime_stg as STRT_DT,  

cast(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6)) as END_DT

FROM 

(SELECT pc_address.CreateTime_stg, pc_address.PostalCode_stg

FROM DB_T_PROD_STAG.pc_address 

WHERE pc_address.UpdateTime_stg> (:start_dttm) and pc_address.UpdateTime_stg <= (:end_dttm)) pc_address 

where postalcode is not null

Group by 1,2,3,4

/**** Remove join with pctl_country******/

/*SELECT   pctl_country.TYPECODE as ctry_TYPECODE, pc_address.PostalCode, 

pc_address.createtime as STRT_DT, cast(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6)) as END_DT

FROM DB_T_PROD_STAG.pctl_country, DB_T_PROD_STAG.pc_address 

WHERE  pctl_country.ID=pc_address.Country and pc_address.PostalCode is not null*/

UNION

/* BOP AND CHURCH -POLICY LOCATION*/

SELECT

''US'' as ctry_TYPECODE , 

SUBSTR(PostalCodeInternal_stg,1, 

CASE	WHEN POSITION(''-'',PostalCodeInternal_stg) = 0

			THEN LENGTH(PostalCodeInternal_stg)

		ELSE POSITION(''-'',PostalCodeInternal_Stg)-1

			END) as PostalCode,

createtime_stg as STRT_DT,  

cast(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6)) as END_DT

FROM 

(SELECT pc_policylocation.CreateTime_stg,pc_policylocation.PostalCodeInternal_stg

FROM DB_T_PROD_STAG.pc_policylocation

where pc_policylocation.UpdateTime_stg > (:start_dttm) and pc_policylocation.UpdateTime_stg <= (:end_dttm)) pc_policylocation 

where PostalCodeInternal_stg is not null 

UNION

SELECT  pc_loc_master_x.COUNTRY_CODE as ctry_typecode,   pc_loc_master_x.ZIP as postalcode,

cast(''01/01/1900'' as date) as STRT_DT, cast(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6)) as END_DT

FROM (select state1.country_typecode as COUNTRY_CODE , 

zip.name_stg as zip

from 

(select pc_zone.id_stg, pctl_country.TYPECODE_stg as country_typecode 

from (select id_stg,ZoneType_stg,Country_stg from DB_T_PROD_STAG.pc_zone) pc_zone inner join (select id_stg,TYPECODE_stg from DB_T_PROD_STAG.pctl_zonetype) pctl_zonetype on pctl_zonetype.id_stg=pc_zone.ZoneType_stg

inner join (select TYPECODE_stg,id_stg from DB_T_PROD_STAG.pctl_country) pctl_country on pctl_country.id_stg=pc_zone.Country_stg

where pctl_zonetype.TYPECODE_stg=''state'') state1,



(select pc_zone.id_stg,pc_zone.name_stg

from (select id_stg,name_stg,ZoneType_stg,Country_stg from DB_T_PROD_STAG.pc_zone) pc_zone inner join (select id_stg,TYPECODE_stg from DB_T_PROD_STAG.pctl_zonetype) pctl_zonetype on pctl_zonetype.id_stg=pc_zone.ZoneType_stg

inner join (select id_stg from DB_T_PROD_STAG.pctl_country) pctl_country on pctl_country.id_stg=pc_zone.Country_stg

where pctl_zonetype.TYPECODE_stg=''county'') county,



(select pc_zone.id_stg,pc_zone.name_stg

from (select id_stg,name_stg,ZoneType_stg,Country_stg from DB_T_PROD_STAG.pc_zone) pc_zone inner join (select id_stg,TYPECODE_stg from DB_T_PROD_STAG.pctl_zonetype) pctl_zonetype on pctl_zonetype.id_stg=pc_zone.ZoneType_stg

inner join (select id_stg from DB_T_PROD_STAG.pctl_country) pctl_country on pctl_country.id_stg=pc_zone.Country_stg

where pctl_zonetype.TYPECODE_stg=''city'') city,



(select pc_zone.id_stg,pc_zone.name_stg

from (select id_stg,name_stg,ZoneType_stg,Country_stg from DB_T_PROD_STAG.pc_zone) pc_zone inner join (select id_stg,TYPECODE_stg from DB_T_PROD_STAG.pctl_zonetype) pctl_zonetype on pctl_zonetype.id_stg=pc_zone.ZoneType_stg

inner join (select id_stg from DB_T_PROD_STAG.pctl_country) pctl_country on pctl_country.id_stg=pc_zone.Country_stg

where pctl_zonetype.TYPECODE_stg=''zip'') zip,

(select Zone1Id_stg,Zone2Id_stg from DB_T_PROD_STAG.pc_zone_link) zl_state_county,

(select Zone1Id_stg,Zone2Id_stg from DB_T_PROD_STAG.pc_zone_link) zl_county_city,

(select Zone1Id_stg,Zone2Id_stg from DB_T_PROD_STAG.pc_zone_link) zl_city_zip

where 

state1.id_stg=zl_state_county.Zone1Id_stg and zl_state_county.Zone2Id_stg=county.id_stg

and county.id_stg=zl_county_city.Zone1Id_stg and zl_county_city.Zone2Id_stg=city.ID_stg

and city.id_stg=zl_city_zip.Zone1Id_stg and zl_city_zip.Zone2Id_stg=zip.id_stg

/*order by 1,2*/)pc_loc_master_x 

) x 

group by case when ctry_TYPECODE is null then ''US'' else ctry_TYPECODE end,  PostalCode, END_DT
) SRC
)
);


-- Component exp_pass_from_source_pc1111, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_from_source_pc1111 AS
(
SELECT
sq_cc_address.ctry_typecode as ctry_TYPECODE,
sq_cc_address.PostalCode as PostalCode,
sq_cc_address.Strt_dt as Strt_Dt,
sq_cc_address.End_dt as End_Dt,
NULL as Retired,
sq_cc_address.source_record_id
FROM
sq_cc_address
);


-- Component exp_id_lookup_pc1111, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_id_lookup_pc1111 AS
(
SELECT
exp_pass_from_source_pc1111.PostalCode as PostalCode,
exp_pass_from_source_pc1111.Strt_Dt as Strt_Dt,
exp_pass_from_source_pc1111.End_Dt as End_Dt,
LKP_1.CTRY_ID /* replaced lookup LKP_CTRY */ as v_ctry_id,
NULL as v_terr_id,
NULL as v_cnty_id,
LKP_2.POSTL_CD_ID /* replaced lookup LKP_POSTALCODE */ as v_postl_cd_id,
''LOCTR_SBTYPE3'' as v_loctr_sbtype_val,
''GEOGRCL_AREA_SBTYPE4'' as v_geogrcl_area_sbtype_val,
LKP_3.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_LOCTR_SBTYPE */ as v_loctr_sbtype,
LKP_4.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_GEOGRCL_AREA_SBTYPE */ as v_geogrcl_area_sbtype,
exp_pass_from_source_pc1111.Retired as Retired,
v_ctry_id as o_ctry_id,
v_cnty_id as o_cnty_id,
v_postl_cd_id as o_postl_cd_id,
v_loctr_sbtype as o_loctr_sbtype,
v_geogrcl_area_sbtype as o_geogrcl_area_sbtype,
:PRCS_ID as o_proces_id,
CURRENT_TIMESTAMP as out_edw_strt_dttm,
exp_pass_from_source_pc1111.source_record_id,
row_number() over (partition by exp_pass_from_source_pc1111.source_record_id order by exp_pass_from_source_pc1111.source_record_id) as RNK
FROM
exp_pass_from_source_pc1111
LEFT JOIN LKP_CTRY LKP_1 ON LKP_1.GEOGRCL_AREA_SHRT_NAME = exp_pass_from_source_pc1111.ctry_TYPECODE
LEFT JOIN LKP_POSTALCODE LKP_2 ON LKP_2.CTRY_ID = v_ctry_id AND LKP_2.POSTL_CD_NUM = exp_pass_from_source_pc1111.PostalCode
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_LOCTR_SBTYPE LKP_3 ON LKP_3.SRC_IDNTFTN_VAL = v_loctr_sbtype_val
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_GEOGRCL_AREA_SBTYPE LKP_4 ON LKP_4.SRC_IDNTFTN_VAL = v_geogrcl_area_sbtype_val
QUALIFY RNK = 1
);


-- Component LKP_POSTL_CD_CDC, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_POSTL_CD_CDC AS
(
SELECT
LKP.POSTL_CD_ID,
LKP.CTRY_ID,
LKP.POSTL_CD_NUM,
LKP.GEOGRCL_AREA_STRT_DTTM,
LKP.GEOGRCL_AREA_END_DTTM,
LKP.LOCTR_SBTYPE_CD,
LKP.GEOGRCL_AREA_SBTYPE_CD,
exp_id_lookup_pc1111.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_id_lookup_pc1111.source_record_id ORDER BY LKP.POSTL_CD_ID asc,LKP.CTRY_ID asc,LKP.POSTL_CD_NUM asc,LKP.GEOGRCL_AREA_STRT_DTTM asc,LKP.GEOGRCL_AREA_END_DTTM asc,LKP.LOCTR_SBTYPE_CD asc,LKP.GEOGRCL_AREA_SBTYPE_CD asc) RNK
FROM
exp_id_lookup_pc1111
LEFT JOIN (
SELECT POSTL_CD.POSTL_CD_ID AS POSTL_CD_ID
,POSTL_CD.CTRY_ID AS CTRY_ID
,POSTL_CD.POSTL_CD_NUM AS POSTL_CD_NUM
,POSTL_CD.GEOGRCL_AREA_STRT_DTTM AS GEOGRCL_AREA_STRT_DTTM
,POSTL_CD.GEOGRCL_AREA_END_DTTM AS GEOGRCL_AREA_END_DTTM
,POSTL_CD.LOCTR_SBTYPE_CD AS LOCTR_SBTYPE_CD
,POSTL_CD.GEOGRCL_AREA_SBTYPE_CD AS GEOGRCL_AREA_SBTYPE_CD
FROM DB_T_PROD_CORE.POSTL_CD 
WHERE CAST(POSTL_CD.EDW_END_DTTM AS DATE)=CAST(''9999-12-31'' AS DATE)
) LKP ON LKP.POSTL_CD_NUM = exp_id_lookup_pc1111.PostalCode AND LKP.CTRY_ID = exp_id_lookup_pc1111.o_ctry_id
QUALIFY RNK = 1
);


-- Component exp_ins_upd, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_ins_upd AS
(
SELECT
exp_id_lookup_pc1111.PostalCode as PostalCode,
exp_id_lookup_pc1111.o_ctry_id as o_ctry_id,
exp_id_lookup_pc1111.o_cnty_id as o_cnty_id,
exp_id_lookup_pc1111.o_postl_cd_id as o_postl_cd_id,
exp_id_lookup_pc1111.o_loctr_sbtype as o_loctr_sbtype,
exp_id_lookup_pc1111.o_geogrcl_area_sbtype as o_geogrcl_area_sbtype,
exp_id_lookup_pc1111.Strt_Dt as Strt_Dt,
exp_id_lookup_pc1111.End_Dt as End_Dt,
exp_id_lookup_pc1111.Retired as Retired,
exp_id_lookup_pc1111.o_proces_id as o_proces_id,
LKP_POSTL_CD_CDC.POSTL_CD_ID as lkp_POSTL_CD_ID,
LKP_POSTL_CD_CDC.CTRY_ID as lkp_CTRY_ID,
LKP_POSTL_CD_CDC.POSTL_CD_NUM as lkp_POSTL_CD_NUM,
LKP_POSTL_CD_CDC.GEOGRCL_AREA_STRT_DTTM as lkp_GEOGRCL_AREA_STRT_DT,
LKP_POSTL_CD_CDC.GEOGRCL_AREA_END_DTTM as lkp_GEOGRCL_AREA_END_DT,
LKP_POSTL_CD_CDC.LOCTR_SBTYPE_CD as lkp_LOCTR_SBTYPE_CD,
LKP_POSTL_CD_CDC.GEOGRCL_AREA_SBTYPE_CD as lkp_GEOGRCL_AREA_SBTYPE_CD,
MD5 ( rtrim ( ltrim ( LKP_POSTL_CD_CDC.POSTL_CD_ID ) ) || to_char ( rtrim ( ltrim ( LKP_POSTL_CD_CDC.GEOGRCL_AREA_STRT_DTTM ) ) ) || to_char ( rtrim ( ltrim ( LKP_POSTL_CD_CDC.GEOGRCL_AREA_END_DTTM ) ) ) || rtrim ( ltrim ( LKP_POSTL_CD_CDC.LOCTR_SBTYPE_CD ) ) || rtrim ( ltrim ( LKP_POSTL_CD_CDC.GEOGRCL_AREA_SBTYPE_CD ) ) ) as var_orig_chksm,
MD5 ( rtrim ( ltrim ( exp_id_lookup_pc1111.o_postl_cd_id ) ) || to_char ( rtrim ( ltrim ( exp_id_lookup_pc1111.Strt_Dt ) ) ) || to_char ( rtrim ( ltrim ( exp_id_lookup_pc1111.End_Dt ) ) ) || rtrim ( ltrim ( exp_id_lookup_pc1111.o_loctr_sbtype ) ) || rtrim ( ltrim ( exp_id_lookup_pc1111.o_geogrcl_area_sbtype ) ) ) as var_calc_chksm,
CASE WHEN var_orig_chksm IS NULL THEN ''I'' ELSE CASE WHEN var_orig_chksm != var_calc_chksm THEN ''U'' ELSE ''R'' END END as o_upd_or_ins,
exp_id_lookup_pc1111.out_edw_strt_dttm as out_edw_strt_dttm,
exp_id_lookup_pc1111.source_record_id
FROM
exp_id_lookup_pc1111
INNER JOIN LKP_POSTL_CD_CDC ON exp_id_lookup_pc1111.source_record_id = LKP_POSTL_CD_CDC.source_record_id
);


-- Component rtr_update_insert_pc1111_INSERT, Type ROUTER Output Group INSERT
CREATE OR REPLACE TEMPORARY TABLE rtr_update_insert_pc1111_INSERT AS
(SELECT
exp_ins_upd.PostalCode as PostalCode,
exp_ins_upd.o_ctry_id as o_ctry_id,
exp_ins_upd.o_cnty_id as o_cnty_id,
exp_ins_upd.o_postl_cd_id as o_postl_cd_id,
exp_ins_upd.o_loctr_sbtype as o_loctr_sbtype,
exp_ins_upd.o_geogrcl_area_sbtype as o_geogrcl_area_sbtype,
exp_ins_upd.o_proces_id as o_proces_id,
exp_ins_upd.o_upd_or_ins as o_upd_or_ins,
exp_ins_upd.Strt_Dt as Strt_Dt,
exp_ins_upd.End_Dt as End_Dt,
exp_ins_upd.Retired as Retired,
exp_ins_upd.lkp_POSTL_CD_ID as lkp_POSTL_CD_ID,
exp_ins_upd.lkp_CTRY_ID as lkp_CTRY_ID,
exp_ins_upd.lkp_POSTL_CD_NUM as lkp_POSTL_CD_NUM,
exp_ins_upd.lkp_GEOGRCL_AREA_STRT_DT as lkp_GEOGRCL_AREA_STRT_DT,
exp_ins_upd.lkp_GEOGRCL_AREA_END_DT as lkp_GEOGRCL_AREA_END_DT,
exp_ins_upd.lkp_LOCTR_SBTYPE_CD as lkp_LOCTR_SBTYPE_CD,
exp_ins_upd.lkp_GEOGRCL_AREA_SBTYPE_CD as lkp_GEOGRCL_AREA_SBTYPE_CD,
exp_ins_upd.out_edw_strt_dttm as out_edw_strt_dttm,
exp_ins_upd.source_record_id
FROM
exp_ins_upd
WHERE exp_ins_upd.o_upd_or_ins = ''I'' -- OR exp_ins_upd.o_upd_or_ins = ''U'' -- OR ( exp_ins_upd.Retired = 0 AND lkp_EDW_END_DTTM != TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) )
);


-- Component upd_insert_pc1111, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_insert_pc1111 AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_update_insert_pc1111_INSERT.o_ctry_id as o_ctry_id1,
rtr_update_insert_pc1111_INSERT.o_cnty_id as o_cnty_id1,
rtr_update_insert_pc1111_INSERT.PostalCode as PostalCode1,
rtr_update_insert_pc1111_INSERT.o_proces_id as o_proces_id1,
rtr_update_insert_pc1111_INSERT.o_loctr_sbtype as o_loctr_sbtype1,
rtr_update_insert_pc1111_INSERT.o_geogrcl_area_sbtype as o_geogrcl_area_sbtype1,
rtr_update_insert_pc1111_INSERT.Strt_Dt as Strt_Dt1,
rtr_update_insert_pc1111_INSERT.End_Dt as End_Dt1,
rtr_update_insert_pc1111_INSERT.Retired as Retired1,
SEQ_LOC.NEXTVAL as NEXTVAL,
rtr_update_insert_pc1111_INSERT.out_edw_strt_dttm as out_edw_strt_dttm1,
0 as UPDATE_STRATEGY_ACTION,
rtr_update_insert_pc1111_INSERT.source_record_id
FROM
rtr_update_insert_pc1111_INSERT
);


-- Component exp_pass_to_target_insert_pc1111, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_target_insert_pc1111 AS
(
SELECT
upd_insert_pc1111.o_ctry_id1 as o_ctry_id1,
upd_insert_pc1111.o_cnty_id1 as o_cnty_id1,
upd_insert_pc1111.PostalCode1 as PostalCode1,
upd_insert_pc1111.o_proces_id1 as o_proces_id1,
--.NEXTVAL 
1 as var_POSTL_CD_ID,
var_POSTL_CD_ID as out_POSTL_CD_ID,
upd_insert_pc1111.o_loctr_sbtype1 as o_loctr_sbtype1,
upd_insert_pc1111.o_geogrcl_area_sbtype1 as o_geogrcl_area_sbtype1,
upd_insert_pc1111.Strt_Dt1 as Strt_Dt1,
upd_insert_pc1111.End_Dt1 as End_Dt1,
upd_insert_pc1111.out_edw_strt_dttm1 as o_EDW_STRT_DTTM,
TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as o_EDW_END_DTTM,
upd_insert_pc1111.source_record_id
FROM
upd_insert_pc1111
);


-- Component tgt_postl_cd_insert_bc, Type TARGET 
INSERT INTO DB_T_PROD_CORE.POSTL_CD
(
POSTL_CD_ID,
CNTY_ID,
CTRY_ID,
POSTL_CD_NUM,
GEOGRCL_AREA_STRT_DTTM,
GEOGRCL_AREA_END_DTTM,
LOCTR_SBTYPE_CD,
GEOGRCL_AREA_SBTYPE_CD,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM
)
SELECT
exp_pass_to_target_insert_pc1111.out_POSTL_CD_ID as POSTL_CD_ID,
exp_pass_to_target_insert_pc1111.o_cnty_id1 as CNTY_ID,
exp_pass_to_target_insert_pc1111.o_ctry_id1 as CTRY_ID,
exp_pass_to_target_insert_pc1111.PostalCode1 as POSTL_CD_NUM,
exp_pass_to_target_insert_pc1111.Strt_Dt1 as GEOGRCL_AREA_STRT_DTTM,
exp_pass_to_target_insert_pc1111.End_Dt1 as GEOGRCL_AREA_END_DTTM,
exp_pass_to_target_insert_pc1111.o_loctr_sbtype1 as LOCTR_SBTYPE_CD,
exp_pass_to_target_insert_pc1111.o_geogrcl_area_sbtype1 as GEOGRCL_AREA_SBTYPE_CD,
exp_pass_to_target_insert_pc1111.o_proces_id1 as PRCS_ID,
exp_pass_to_target_insert_pc1111.o_EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_pass_to_target_insert_pc1111.o_EDW_END_DTTM as EDW_END_DTTM
FROM
exp_pass_to_target_insert_pc1111;


-- Component tgt_postl_cd_insert_bc, Type Post SQL 
/*UPDATE POSTL_CD FROM

(SELECT	distinct POSTL_CD_NUM,CTRY_ID,EDW_STRT_DTTM,

max(EDW_STRT_DTTM) over (partition by POSTL_CD_NUM,CTRY_ID 

ORDER	BY EDW_STRT_DTTM ASC rows between 1 following 

	and	1 following) - INTERVAL ''1'' SECOND 

 as lead1

FROM POSTL_CD) A

SET EDW_END_DTTM = A.lead1

WHERE

POSTL_CD.EDW_STRT_DTTM = A.EDW_STRT_DTTM 

AND POSTL_CD.POSTL_CD_NUM = A.POSTL_CD_NUM

AND POSTL_CD.CTRY_ID = A.CTRY_ID

AND A.lead1 is not null; */;


END; ';