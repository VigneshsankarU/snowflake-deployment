-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_CITY_TO_CNTY_INSUPD("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
DECLARE start_dttm TIMESTAMP;
end_dttm TIMESTAMP;
PRCS_ID INTEGER;
FS_DATE date;
BEGIN 
start_dttm := CURRENT_TIMESTAMP();
end_dttm := CURRENT_TIMESTAMP();
PRCS_ID := 1;  
FS_DATE :=current_date();

-- Component LKP_CITY, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_CITY AS
(
SELECT CITY.CITY_ID as CITY_ID, CITY.EDW_STRT_DTTM as EDW_STRT_DTTM, CITY.EDW_END_DTTM as EDW_END_DTTM, CITY.TERR_ID as TERR_ID, CITY.GEOGRCL_AREA_SHRT_NAME as GEOGRCL_AREA_SHRT_NAME 
FROM DB_T_PROD_CORE.CITY 
QUALIFY ROW_NUMBER() OVER(PARTITION BY TERR_ID, GEOGRCL_AREA_SHRT_NAME  
ORDER BY EDW_END_DTTM desc) = 1
/* WHERE CITY.EDW_END_DTTM=TO_TIMESTAMP(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'') */
);


-- Component LKP_COUNTY, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_COUNTY AS
(
SELECT CNTY.CNTY_ID as CNTY_ID, CNTY.GEOGRCL_AREA_SBTYPE_CD as GEOGRCL_AREA_SBTYPE_CD, CNTY.LOCTR_SBTYPE_CD as LOCTR_SBTYPE_CD, CNTY.GEOGRCL_AREA_STRT_DTTM as GEOGRCL_AREA_STRT_DTTM, CNTY.EDW_STRT_DTTM as EDW_STRT_DTTM, CNTY.EDW_END_DTTM as EDW_END_DTTM, CNTY.TERR_ID as TERR_ID, CNTY.GEOGRCL_AREA_SHRT_NAME as GEOGRCL_AREA_SHRT_NAME 
FROM DB_T_PROD_CORE.CNTY
QUALIFY ROW_NUMBER() OVER(PARTITION BY TERR_ID,GEOGRCL_AREA_SHRT_NAME ORDER BY EDW_END_DTTM desc) = 1
);


-- Component LKP_CTRY, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_CTRY AS
(
SELECT CTRY.CTRY_ID as CTRY_ID, CTRY.GEOGRCL_AREA_NAME as GEOGRCL_AREA_NAME, CTRY.GEOGRCL_AREA_DESC as GEOGRCL_AREA_DESC, CTRY.EDW_STRT_DTTM as EDW_STRT_DTTM, CTRY.EDW_END_DTTM as EDW_END_DTTM, CTRY.GEOGRCL_AREA_SHRT_NAME as GEOGRCL_AREA_SHRT_NAME 
FROM DB_T_PROD_CORE.CTRY
QUALIFY ROW_NUMBER() OVER(PARTITION BY GEOGRCL_AREA_SHRT_NAME 
ORDER BY EDW_END_DTTM desc) = 1
/* WHERE CTRY.EDW_END_DTTM=TO_DATE(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'') */
);


-- Component LKP_TERR, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERR AS
(
SELECT TERR.TERR_ID as TERR_ID, TERR.GEOGRCL_AREA_NAME as GEOGRCL_AREA_NAME, TERR.GEOGRCL_AREA_DESC as GEOGRCL_AREA_DESC, TERR.GEOGRCL_AREA_STRT_DTTM as GEOGRCL_AREA_STRT_DTTM, TERR.GEOGRCL_AREA_END_DTTM as GEOGRCL_AREA_END_DTTM, TERR.LOCTR_SBTYPE_CD as LOCTR_SBTYPE_CD, TERR.GEOGRCL_AREA_SBTYPE_CD as GEOGRCL_AREA_SBTYPE_CD, TERR.EDW_STRT_DTTM as EDW_STRT_DTTM, TERR.EDW_END_DTTM as EDW_END_DTTM, TERR.CTRY_ID as CTRY_ID, TERR.GEOGRCL_AREA_SHRT_NAME as GEOGRCL_AREA_SHRT_NAME FROM DB_T_PROD_CORE.TERR
QUALIFY ROW_NUMBER () OVER (PARTITION BY CTRY_ID,GEOGRCL_AREA_SHRT_NAME ORDER BY edw_end_dttm DESC)=1
);


-- Component sq_pc_address, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_pc_address AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as state_TYPECODE,
$2 as ctry_TYPECODE,
$3 as City,
$4 as County,
$5 as STRT_DT,
$6 as END_DT,
$7 as Retired,
$8 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT state_TYPECODE, ctry_TYPECODE,  city, county, MAX(STRT_DT), END_DT,Retired FROM (

SELECT  UPPER(state_TYPECODE) AS state_TYPECODE, UPPER(ctry_TYPECODE) AS ctry_TYPECODE,  UPPER(city) AS city, UPPER(county) AS county, STRT_DT, END_DT,Retired

FROM (

SELECT  bs.TYPECODE_stg AS state_TYPECODE, bc.TYPECODE_stg AS ctry_TYPECODE,  ba.city_stg as city, ba.county_stg as county, 

ba.CreateTime_stg AS STRT_DT, CAST(''12/31/9999'' AS DATE ) AS END_DT,ba.Retired_stg as retired

FROM DB_T_PROD_STAG.bctl_state bs, DB_T_PROD_STAG.bctl_country bc, DB_T_PROD_STAG.bc_address ba 

WHERE ba.State_stg=bs.ID_stg AND bc.ID_stg=ba.Country_stg

AND ba.County_stg IS NOT NULL AND ba.city_stg IS NOT NULL

and ba.UpdateTime_stg>(:start_dttm) AND ba.UpdateTime_stg <= (:end_dttm)

UNION

SELECT cs1.TYPECODE_stg AS state_TYPECODE, cc.TYPECODE_stg AS ctry_TYPECODE,  ca.city_stg as city, ca.county_stg as county, 

ca.CreateTime_stg AS STRT_DT, CAST(''12/31/9999'' AS DATE ) AS END_DT,ca.Retired_stg as retired

FROM DB_T_PROD_STAG.cctl_state cs1, DB_T_PROD_STAG.cctl_country cc, DB_T_PROD_STAG.cc_address ca 

WHERE ca.State_stg=cs1.ID_stg AND cc.ID_stg=ca.Country_stg

AND ca.County_stg IS NOT NULL AND ca.city_stg IS NOT NULL

AND ca.UpdateTime_stg>(:start_dttm) AND ca.UpdateTime_stg <= (:end_dttm)

UNION

SELECT ps.TYPECODE_stg AS state_TYPECODE, pc.TYPECODE_stg AS ctry_TYPECODE, pa.city_stg as city, pa.county_stg as county, 

pa.CreateTime_stg AS STRT_DT, CAST(''12/31/9999'' AS DATE ) AS END_DT,pa.Retired_stg as retired

FROM DB_T_PROD_STAG.pctl_state ps, DB_T_PROD_STAG.pctl_country pc, DB_T_PROD_STAG.pc_address pa 

WHERE pa.State_stg=ps.ID_stg AND pc.ID_stg=pa.Country_stg

AND pa.County_stg IS NOT NULL AND pa.city_stg IS NOT NULL

AND	pa.UpdateTime_stg> (:start_dttm)	and	pa.UpdateTime_stg <= (:end_dttm)



UNION

/* BOP AND CHURCH -POLICY LOCATION*/

SELECT ps.TYPECODE_stg AS state_TYPECODE, pc.TYPECODE_stg AS ctry_TYPECODE, pp.CityInternal_stg AS city, pp.CountyInternal_stg AS County, 

pp.CreateTime_stg AS STRT_DT, CAST(''12/31/9999'' AS DATE ) AS END_DT,

0 As Retired

FROM DB_T_PROD_STAG.pctl_state ps, DB_T_PROD_STAG.pctl_country pc, DB_T_PROD_STAG.pc_policylocation  pp 

WHERE pp.StateInternal_stg=ps.ID_stg AND pc.ID_stg=pp.CountryInternal_stg

AND pp.CountyInternal_stg IS NOT NULL AND pp.CityInternal_stg IS NOT NULL

AND pp.UpdateTime_stg > (:start_dttm)	and pp.UpdateTime_stg <= (:end_dttm)



UNION

SELECT plm.state_code, plm.country_code AS ctry_typecode,city AS city, plm.county AS county, 

CAST(''01/01/1900'' AS DATE ) AS STRT_DT, CAST(''12/31/9999'' AS DATE ) AS END_DT,0 AS Retired

FROM 

(select state1.state_typecode_stg AS state_code,state1.country_typecode_stg AS country_code, 

city.Name_stg as city,county.name_stg as county

from 

(select pc_zone.id_stg,pc_zone.name_stg as state_typecode_stg, pctl_state.name_stg as state_name,

pctl_state.DESCRIPTION_stg,pctl_country.TYPECODE_stg as country_typecode_stg, pctl_country.NAME_stg as cntry_name_stg, pctl_country.DESCRIPTION_stg as cntry_desc

from DB_T_PROD_STAG.pc_zone inner join DB_T_PROD_STAG.pctl_zonetype on pctl_zonetype.id_stg=pc_zone.ZoneType_stg

inner join DB_T_PROD_STAG.pctl_country on pctl_country.id_stg=pc_zone.Country_stg

inner join DB_T_PROD_STAG.pctl_state on pctl_state.TYPECODE_stg=pc_zone.name_stg

where pctl_zonetype.TYPECODE_stg=''state'') state1,

(select pc_zone.id_stg,pc_zone.name_stg

from DB_T_PROD_STAG.pc_zone inner join DB_T_PROD_STAG.pctl_zonetype on pctl_zonetype.id_stg=pc_zone.ZoneType_stg

inner join DB_T_PROD_STAG.pctl_country on pctl_country.id_stg=pc_zone.Country_stg

where pctl_zonetype.TYPECODE_stg=''county'') county,

(select pc_zone.id_stg,pc_zone.name_stg

from DB_T_PROD_STAG.pc_zone inner join DB_T_PROD_STAG.pctl_zonetype on pctl_zonetype.id_stg=pc_zone.ZoneType_stg

inner join DB_T_PROD_STAG.pctl_country on pctl_country.id_stg=pc_zone.Country_stg

where pctl_zonetype.TYPECODE_stg=''city'') city,

(select pc_zone.id_stg,pc_zone.name_stg

from DB_T_PROD_STAG.pc_zone inner join DB_T_PROD_STAG.pctl_zonetype on pctl_zonetype.id_stg=pc_zone.ZoneType_stg

inner join DB_T_PROD_STAG.pctl_country on pctl_country.id_stg=pc_zone.Country_stg

where pctl_zonetype.TYPECODE_stg=''zip'') zip,



DB_T_PROD_STAG.pc_zone_link zl_state_county,

DB_T_PROD_STAG.pc_zone_link zl_county_city,

DB_T_PROD_STAG.pc_zone_link zl_city_zip

where 

state1.id_stg=zl_state_county.Zone1Id_stg and zl_state_county.Zone2Id_stg=county.id_stg

and county.id_stg=zl_county_city.Zone1Id_stg and zl_county_city.Zone2Id_stg=city.ID_stg

and city.id_stg=zl_city_zip.Zone1Id_stg and zl_city_zip.Zone2Id_stg=zip.id_stg

) plm

WHERE plm.county IS NOT NULL AND city IS NOT NULL



/*UNION

SELECT ps.TYPECODE_stg AS state_TYPECODE, pc.TYPECODE_stg AS ctry_TYPECODE, pp.CityInternal_stg AS city, pp.CountyInternal_stg AS County, 

pp.CreateTime_stg AS STRT_DT, CAST(''12/31/9999'' AS DATE ) AS END_DT,

0 As Retired

FROM DB_T_PROD_STAG.pctl_state ps, DB_T_PROD_STAG.pctl_country pc, DB_T_PROD_STAG.pc_policylocation pp 

WHERE pp.StateInternal_stg=ps.ID_stg AND pc.ID_stg=pp.CountryInternal_stg

AND pp.CountyInternal_stg IS NOT NULL AND pp.CityInternal_stg IS NOT NULL

AND pp.UpdateTime_stg > (:start_dttm)	and pp.UpdateTime_stg <= (:end_dttm)*/

) x ) AS A 

GROUP BY state_TYPECODE, ctry_TYPECODE,  city, county, END_DT,Retired

order by state_TYPECODE, ctry_TYPECODE,  city, county, max(STRT_DT)
) SRC
)
);


-- Component exp_pass_from_source_pc1111, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_from_source_pc1111 AS
(
SELECT
sq_pc_address.state_TYPECODE as state_TYPECODE,
sq_pc_address.ctry_TYPECODE as ctry_TYPECODE,
sq_pc_address.City as City,
sq_pc_address.County as County,
sq_pc_address.STRT_DT as STRT_DT,
sq_pc_address.END_DT as END_DT,
sq_pc_address.Retired as Retired,
sq_pc_address.source_record_id
FROM
sq_pc_address
);


-- Component exp_id_lookup_pc1111, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_id_lookup_pc1111 AS
(
SELECT
LKP_1.CTRY_ID /* replaced lookup LKP_CTRY */ as v_ctry_id,
LKP_2.TERR_ID /* replaced lookup LKP_TERR */ as v_terr_id,
LKP_3.CNTY_ID /* replaced lookup LKP_COUNTY */ as v_cnty_id,
LKP_4.CITY_ID /* replaced lookup LKP_CITY */ as v_city_id,
exp_pass_from_source_pc1111.STRT_DT as STRT_DT,
exp_pass_from_source_pc1111.END_DT as END_DT,
CASE WHEN v_city_id IS NULL THEN 9999 ELSE v_city_id END as o_city_id,
CASE WHEN v_cnty_id IS NULL THEN 9999 ELSE v_cnty_id END as o_cnty_id,
:PRCS_ID as PRCS_ID,
exp_pass_from_source_pc1111.Retired as Retired,
exp_pass_from_source_pc1111.source_record_id,
row_number() over (partition by exp_pass_from_source_pc1111.source_record_id order by exp_pass_from_source_pc1111.source_record_id) as RNK
FROM
exp_pass_from_source_pc1111
LEFT JOIN LKP_CTRY LKP_1 ON LKP_1.GEOGRCL_AREA_SHRT_NAME = exp_pass_from_source_pc1111.ctry_TYPECODE
LEFT JOIN LKP_TERR LKP_2 ON LKP_2.CTRY_ID = v_ctry_id AND LKP_2.GEOGRCL_AREA_SHRT_NAME = exp_pass_from_source_pc1111.state_TYPECODE
LEFT JOIN LKP_COUNTY LKP_3 ON LKP_3.TERR_ID = v_terr_id AND LKP_3.GEOGRCL_AREA_SHRT_NAME = exp_pass_from_source_pc1111.County
LEFT JOIN LKP_CITY LKP_4 ON LKP_4.TERR_ID = v_terr_id AND LKP_4.GEOGRCL_AREA_SHRT_NAME = exp_pass_from_source_pc1111.City
QUALIFY RNK = 1
);


-- Component LKP_CITY_TO_CNTY_CDC, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_CITY_TO_CNTY_CDC AS
(
SELECT
LKP.CITY_ID,
LKP.CNTY_ID,
LKP.CITY_TO_CNTY_STRT_DTTM,
exp_id_lookup_pc1111.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_id_lookup_pc1111.source_record_id ORDER BY LKP.CITY_ID asc,LKP.CNTY_ID asc,LKP.CITY_TO_CNTY_STRT_DTTM asc) RNK
FROM
exp_id_lookup_pc1111
LEFT JOIN (
SELECT CITY_TO_CNTY.CITY_TO_CNTY_STRT_DTTM as CITY_TO_CNTY_STRT_DTTM, CITY_TO_CNTY.CITY_ID as CITY_ID, CITY_TO_CNTY.CNTY_ID as CNTY_ID FROM DB_T_PROD_CORE.CITY_TO_CNTY QUALIFY	ROW_NUMBER() OVER(
PARTITION BY  CITY_TO_CNTY.CITY_ID,CITY_TO_CNTY.CNTY_ID 
ORDER BY CITY_TO_CNTY.EDW_END_DTTM DESC) = 1
) LKP ON LKP.CITY_ID = exp_id_lookup_pc1111.o_city_id AND LKP.CNTY_ID = exp_id_lookup_pc1111.o_cnty_id
QUALIFY RNK = 1
);


-- Component exp_ins_upd, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_ins_upd AS
(
SELECT
LKP_CITY_TO_CNTY_CDC.CITY_ID as lkp_CITY_ID,
LKP_CITY_TO_CNTY_CDC.CNTY_ID as lkp_CNTY_ID,
LKP_CITY_TO_CNTY_CDC.CITY_TO_CNTY_STRT_DTTM as lkp_CITY_TO_CNTY_STRT_DT,
exp_id_lookup_pc1111.o_city_id as city_id,
exp_id_lookup_pc1111.o_cnty_id as cnty_id,
exp_id_lookup_pc1111.STRT_DT as CITY_TO_CNTY_STRT_DT,
exp_id_lookup_pc1111.END_DT as CITY_TO_CNTY_END_DT,
exp_id_lookup_pc1111.PRCS_ID as PRCS_ID,
MD5 ( to_char ( ltrim ( rtrim ( LKP_CITY_TO_CNTY_CDC.CITY_TO_CNTY_STRT_DTTM ) ) ) ) as var_orig_chksm,
MD5 ( to_char ( ltrim ( rtrim ( exp_id_lookup_pc1111.STRT_DT ) ) ) ) as var_calc_chksm,
CASE WHEN var_orig_chksm IS NULL THEN ''I'' ELSE CASE WHEN var_orig_chksm != var_calc_chksm THEN ''U'' ELSE ''R'' END END as out_ins_upd,
exp_id_lookup_pc1111.Retired as Retired,
CURRENT_TIMESTAMP as out_EDW_STRT_DTTM,
exp_id_lookup_pc1111.source_record_id
FROM
exp_id_lookup_pc1111
INNER JOIN LKP_CITY_TO_CNTY_CDC ON exp_id_lookup_pc1111.source_record_id = LKP_CITY_TO_CNTY_CDC.source_record_id
);


-- Component rtr_city_to_cnty_Insert, Type ROUTER Output Group Insert
CREATE OR REPLACE TEMPORARY TABLE rtr_city_to_cnty_Insert AS
(SELECT
exp_ins_upd.lkp_CITY_ID as lkp_CITY_ID,
exp_ins_upd.lkp_CNTY_ID as lkp_CNTY_ID,
exp_ins_upd.lkp_CITY_TO_CNTY_STRT_DT as lkp_CITY_TO_CNTY_STRT_DT,
exp_ins_upd.city_id as city_id_new,
exp_ins_upd.cnty_id as cnty_id_new,
exp_ins_upd.PRCS_ID as PRCS_ID,
exp_ins_upd.CITY_TO_CNTY_STRT_DT as CITY_TO_CNTY_STRT_DT,
exp_ins_upd.CITY_TO_CNTY_END_DT as CITY_TO_CNTY_END_DT,
exp_ins_upd.out_ins_upd as out_ins_upd,
exp_ins_upd.Retired as Retired,
exp_ins_upd.out_EDW_STRT_DTTM as out_EDW_STRT_DTTM,
exp_ins_upd.source_record_id
FROM
exp_ins_upd
WHERE ( exp_ins_upd.out_ins_upd = ''I'' ) 
-- OR ( exp_ins_upd.out_ins_upd = ''U'' AND lkp_EDW_END_DTTM ! 
-- = TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ) 
-- exp_ins_upd.lkp_CITY_ID IS NULL AND exp_ins_upd.lkp_CNTY_ID IS NULL
);


-- Component upd_city_to_cnty_ins, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_city_to_cnty_ins AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_city_to_cnty_Insert.city_id_new as CITY_ID,
rtr_city_to_cnty_Insert.cnty_id_new as CNTY_ID,
rtr_city_to_cnty_Insert.PRCS_ID as PRCS_ID,
rtr_city_to_cnty_Insert.CITY_TO_CNTY_STRT_DT as CITY_TO_CNTY_STRT_DT,
rtr_city_to_cnty_Insert.CITY_TO_CNTY_END_DT as CITY_TO_CNTY_END_DT,
rtr_city_to_cnty_Insert.Retired as Retired1,
rtr_city_to_cnty_Insert.out_EDW_STRT_DTTM as out_EDW_STRT_DTTM1,
0 as UPDATE_STRATEGY_ACTION,
rtr_city_to_cnty_Insert.source_record_id
FROM
rtr_city_to_cnty_Insert
);


-- Component exp_insert, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_insert AS
(
SELECT
upd_city_to_cnty_ins.CITY_ID as CITY_ID,
upd_city_to_cnty_ins.CNTY_ID as CNTY_ID,
upd_city_to_cnty_ins.PRCS_ID as PRCS_ID,
upd_city_to_cnty_ins.CITY_TO_CNTY_STRT_DT as CITY_TO_CNTY_STRT_DT,
upd_city_to_cnty_ins.CITY_TO_CNTY_END_DT as CITY_TO_CNTY_END_DT,
upd_city_to_cnty_ins.out_EDW_STRT_DTTM1 as out_EDW_STRT_DTTM,
CASE WHEN upd_city_to_cnty_ins.Retired1 != 0 THEN upd_city_to_cnty_ins.out_EDW_STRT_DTTM1 ELSE TO_TIMESTAMP ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) END as out_EDW_END_DTTM,
upd_city_to_cnty_ins.source_record_id
FROM
upd_city_to_cnty_ins
);


-- Component tgt_city_to_cnty_pc_insert, Type TARGET 
INSERT INTO DB_T_PROD_CORE.CITY_TO_CNTY
(
CITY_ID,
CNTY_ID,
PRCS_ID,
CITY_TO_CNTY_STRT_DTTM,
CITY_TO_CNTY_END_DTTM,
EDW_STRT_DTTM,
EDW_END_DTTM
)
SELECT
exp_insert.CITY_ID as CITY_ID,
exp_insert.CNTY_ID as CNTY_ID,
exp_insert.PRCS_ID as PRCS_ID,
exp_insert.CITY_TO_CNTY_STRT_DT as CITY_TO_CNTY_STRT_DTTM,
exp_insert.CITY_TO_CNTY_END_DT as CITY_TO_CNTY_END_DTTM,
exp_insert.out_EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_insert.out_EDW_END_DTTM as EDW_END_DTTM
FROM
exp_insert;


END; ';