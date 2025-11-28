-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_CNSUS_TRCT_INSUPD("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' DECLARE start_dttm TIMESTAMP;
end_dttm TIMESTAMP;
PRCS_ID INTEGER;
FS_DATE date;
BEGIN 
start_dttm := CURRENT_TIMESTAMP();
end_dttm := CURRENT_TIMESTAMP();
PRCS_ID := 1;  

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
$7 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select distinct state_TYPECODE,ctry_TYPECD,

county,

max(createtime) createtime,
cast (0 as bigint) Retired,
Retired,TerritoryFIPSCode_alfa  

from (

select 

distinct state_TYPECODE,

case when (ctry_TYPECODE is null) then ''US'' else ctry_TYPECODE end ctry_TYPECD,

county,

max(createtime_stg) createtime,



TRIM(
  CASE
    WHEN TerritoryFIPSCode_alfa IS NULL THEN TerritoryFIPSCode_alfa
    ELSE SUBSTRING(TerritoryFIPSCode_alfa, 6, 6)
  END
) AS TerritoryFIPSCode_alfa 

from (



SELECT  pctl_state.TYPECODE_STG as state_TYPECODE,

pctl_country.TYPECODE_stg as ctry_TYPECODE,

pc_address.county_stg as County,

pc_address.createtime_stg,

CASE WHEN LENGTH(TRIM(TerritoryFIPSCode_alfa_stg))=15 THEN TerritoryFIPSCode_alfa_stg ELSE NULL END AS TerritoryFIPSCode_alfa

FROM 

 

(SELECT    

	pc_address.CreateTime_stg, 

	pc_address.County_stg,

	pc_address.State_stg,

	pc_address.UpdateTime_stg,

	pc_address.Country_stg, 

	TerritoryFIPSCode_alfa_stg

FROM DB_T_PROD_STAG.pc_address WHERE pc_address.UpdateTime_stg> (:START_DTTM) and    pc_address.UpdateTime_stg <= (:end_dttm)

) pc_address left join 

(select

TYPECODE_stg,

ID_stg 

from DB_T_PROD_STAG.pctl_country

) pctl_country on pctl_country.ID_stg=pc_address.Country_stg join 

(select

TYPECODE_stg,

ID_stg 

from DB_T_PROD_STAG.PCTL_STATE

) pctl_state on pc_address.State_stg=pctl_state.ID_stg 

where pc_address.county_stg is not null and TerritoryFIPSCode_alfa_stg is not null



UNION

SELECT	 pctl_state.TYPECODE_stg as state_TYPECODE,

         pctl_country.TYPECODE_stg as ctry_TYPECODE,

		pc_policylocation.CountyInternal_stg AS County,

		pc_policylocation.createtime_stg,

		CASE WHEN LENGTH(TRIM(TerritoryFIPSCode_alfaInternal_stg))=15 THEN TerritoryFIPSCode_alfaInternal_stg ELSE NULL END AS TerritoryFIPSCode_alfa

FROM	 

(SELECT 

    pc_policylocation.CountyInternal_stg,

	pc_policylocation.CreateTime_stg, pc_policylocation.StateInternal_stg, 

	pc_policylocation.TerritoryFIPSCode_alfainternal_stg,CountryInternal_stg

FROM DB_T_PROD_STAG.pc_policylocation 

where pc_policylocation.UpdateTime_stg > (:START_DTTM) 

and pc_policylocation.UpdateTime_stg <= (:end_dttm)

) pc_policylocation 

join 

(select

TYPECODE_stg,

ID_stg 

from DB_T_PROD_STAG.PCTL_STATE) pctl_state on pc_policylocation.StateInternal_stg=pctl_state.ID_stg left join 

(select

TYPECODE_stg,

ID_stg 

from DB_T_PROD_STAG.pctl_country) pctl_country on	pctl_country.ID_stg=pc_policylocation.CountryInternal_stg

    where	pc_policylocation.CountyInternal_stg is not null  and TerritoryFIPSCode_alfaInternal_stg is not null

) a WHERE TerritoryFIPSCode_alfa IS NOT NULL

group by state_TYPECODE,ctry_TYPECD,county,TerritoryFIPSCode_alfa) as a  



group by state_TYPECODE,ctry_TYPECD,county,TerritoryFIPSCode_alfa
) SRC
)
);


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
select 

state_TYPECODE,

case when (ctry_TYPECODE is null) then ''US'' else ctry_TYPECODE end ctry_TYPECD,

county,

max(createtime_stg) as createtime,

cast (0 as bigint) Retired,

TerritoryFIPSCode_alfa  

from (

SELECT  

pctl_state.TYPECODE_stg as state_TYPECODE, 

pctl_country.TYPECODE_stg as ctry_TYPECODE, 

pc_address.county_stg as County,

pc_address.createtime_stg,

pc_address.TerritoryFIPSCode_alfa_stg as TerritoryFIPSCode_alfa

FROM  (SELECT    

	pc_address.CreateTime_stg, 

	pc_address.County_stg,

	pc_address.State_stg,

	pc_address.UpdateTime_stg,

	pc_address.Country_stg, 

	TerritoryFIPSCode_alfa_stg

FROM DB_T_PROD_STAG.pc_address WHERE pc_address.UpdateTime_stg> (:START_DTTM) and    pc_address.UpdateTime_stg <= (:end_dttm)

) pc_address 

left join 

(select

TYPECODE_stg,

ID_stg 

from DB_T_PROD_STAG.pctl_country) pctl_country 

on pctl_country.ID_stg=pc_address.Country_stg 

join (select

TYPECODE_stg,

ID_stg 

from DB_T_PROD_STAG.pctl_state) pctl_state on pc_address.State_stg=pctl_state.ID_stg 

where pc_address.county_stg is not null



UNION



SELECT	 

pctl_state.TYPECODE_stg as state_TYPECODE_stg, 

pctl_country.TYPECODE_stg as ctry_TYPECODE_stg,

		pc_policylocation.CountyInternal_stg AS County,

		pc_policylocation.createtime_stg,

pc_policylocation.TerritoryFIPSCode_alfaInternal_stg as TerritoryFIPSCode_alfa /* TerritoryFIPSCode_alfa */
FROM	 

(SELECT 

    pc_policylocation.CountyInternal_stg,

	pc_policylocation.CreateTime_stg, pc_policylocation.StateInternal_stg, 

	pc_policylocation.TerritoryFIPSCode_alfainternal_stg,CountryInternal_stg

FROM DB_T_PROD_STAG.pc_policylocation 

where pc_policylocation.UpdateTime_stg > (:START_DTTM) 

and pc_policylocation.UpdateTime_stg <= (:end_dttm)

) pc_policylocation 

join 

(select

TYPECODE_stg,

ID_stg 

from DB_T_PROD_STAG.pctl_state) pctl_state 

	on	 pc_policylocation.StateInternal_stg=pctl_state.ID_stg left join 

	(select

TYPECODE_stg,

ID_stg 

from DB_T_PROD_STAG.pctl_country) pctl_country 

	on	pctl_country.ID_stg=pc_policylocation.CountryInternal_stg

where	pc_policylocation.CountyInternal_stg is not null

) a where 1=2

group by state_TYPECODE,ctry_TYPECD,county,TerritoryFIPSCode_alfa
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
LKP_1.CTRY_ID /* replaced lookup LKP_CTRY */ as v_ctry_id,
LKP_2.TERR_ID /* replaced lookup LKP_TERR */ as v_terr_id,
LKP_3.CNTY_ID /* replaced lookup LKP_COUNTY */ as v_cnty_id,
CASE WHEN v_cnty_id IS NULL THEN 9999 ELSE v_cnty_id END as o_cnty_id,
sq_pc_address.CreateTime as CreateTime,
sq_pc_address.Retired as Retired,
sq_pc_address.TerritoryFIPSCode_alfa as o_TerritoryFIPSCode_alfa,
sq_pc_address.source_record_id,
row_number() over (partition by sq_pc_address.source_record_id order by sq_pc_address.source_record_id) as RNK
FROM
sq_pc_address
LEFT JOIN LKP_CTRY LKP_1 ON LKP_1.GEOGRCL_AREA_SHRT_NAME = sq_pc_address.ctry_TYPECODE
LEFT JOIN LKP_TERR LKP_2 ON LKP_2.CTRY_ID = v_ctry_id AND LKP_2.GEOGRCL_AREA_SHRT_NAME = sq_pc_address.state_TYPECODE
LEFT JOIN LKP_COUNTY LKP_3 ON LKP_3.TERR_ID = v_terr_id AND LKP_3.GEOGRCL_AREA_SHRT_NAME = ltrim ( rtrim ( sq_pc_address.County ) )
QUALIFY RNK = 1
);


-- Component exp_pass_through_id1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_through_id1 AS
(
SELECT
LKP_1.CTRY_ID /* replaced lookup LKP_CTRY */ as v_ctry_id,
LKP_2.TERR_ID /* replaced lookup LKP_TERR */ as v_terr_id,
LKP_3.CNTY_ID /* replaced lookup LKP_COUNTY */ as v_cnty_id,
CASE WHEN v_cnty_id IS NULL THEN 9999 ELSE v_cnty_id END as o_cnty_id,
CASE WHEN sq_pc_address1.TerritoryFIPSCode_alfa IS NULL THEN ''0'' ELSE SUBSTR ( sq_pc_address1.TerritoryFIPSCode_alfa , 6 , 6 ) END as v_TerritoryFIPSCode_alfa,
sq_pc_address1.source_record_id,
row_number() over (partition by sq_pc_address1.source_record_id order by sq_pc_address1.source_record_id) as RNK
FROM
sq_pc_address1
LEFT JOIN LKP_CTRY LKP_1 ON LKP_1.GEOGRCL_AREA_SHRT_NAME = sq_pc_address1.ctry_TYPECODE
LEFT JOIN LKP_TERR LKP_2 ON LKP_2.CTRY_ID = v_ctry_id AND LKP_2.GEOGRCL_AREA_SHRT_NAME = sq_pc_address1.state_TYPECODE
LEFT JOIN LKP_COUNTY LKP_3 ON LKP_3.TERR_ID = v_terr_id AND LKP_3.GEOGRCL_AREA_SHRT_NAME = ltrim ( rtrim ( sq_pc_address1.County ) )
QUALIFY RNK = 1
);


-- Component CNSUS_TRCT1, Type TARGET 
INSERT INTO DB_T_PROD_CORE.CNSUS_TRCT
(
CNTY_ID,
CNSUS_TRCT_NUM
)
SELECT
exp_pass_through_id1.o_cnty_id as CNTY_ID,
exp_pass_through_id1.v_TerritoryFIPSCode_alfa as CNSUS_TRCT_NUM
FROM
exp_pass_through_id1;


-- PIPELINE END FOR 2
-- Component CNSUS_TRCT1, Type Post SQL 
UPDATE  DB_T_PROD_CORE.CNSUS_TRCT  FROM  

(

SELECT	distinct CNSUS_TRCT_ID,CNTY_ID,GEOGRCL_AREA_SHRT_NAME,GEOGRCL_AREA_STRT_DT,CNSUS_TRCT_NUM,EDW_STRT_DTTM,

max(EDW_STRT_DTTM) over (partition by CNTY_ID,GEOGRCL_AREA_SHRT_NAME,CNSUS_TRCT_NUM ORDER BY EDW_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND'' 

 as LEAD1

FROM 

DB_T_PROD_CORE.CNSUS_TRCT  

)  A

set EDW_END_DTTM=A.LEAD1

where  CNSUS_TRCT.CNSUS_TRCT_ID=A.CNSUS_TRCT_ID

AND CNSUS_TRCT.CNTY_ID=A.CNTY_ID

and CNSUS_TRCT.GEOGRCL_AREA_SHRT_NAME=A.GEOGRCL_AREA_SHRT_NAME

and CNSUS_TRCT.CNSUS_TRCT_NUM=A.CNSUS_TRCT_NUM

and CNSUS_TRCT.EDW_STRT_DTTM = A.EDW_STRT_DTTM

and CAST(CNSUS_TRCT.EDW_END_DTTM AS DATE)=''9999-12-31''

and LEAD1  is not null;


-- Component LKP_CNSUS_TRCT, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_CNSUS_TRCT AS
(
SELECT
LKP.CNSUS_TRCT_ID,
LKP.GEOGRCL_AREA_SBTYPE_CD,
LKP.LOCTR_SBTYPE_CD,
LKP.CNSUS_TRCT_NUM,
exp_pass_through_id.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_through_id.source_record_id ORDER BY LKP.CNSUS_TRCT_ID desc,LKP.CNTY_ID desc,LKP.GEOGRCL_AREA_SBTYPE_CD desc,LKP.GEOGRCL_AREA_SHRT_NAME desc,LKP.GEOGRCL_AREA_NAME desc,LKP.GEOGRCL_AREA_DESC desc,LKP.CURY_CD desc,LKP.GEOGRCL_AREA_STRT_DT desc,LKP.GEOGRCL_AREA_END_DT desc,LKP.LOCTR_SBTYPE_CD desc,LKP.CNSUS_TRCT_NUM desc,LKP.PRCS_ID desc,LKP.EDW_STRT_DTTM desc,LKP.EDW_END_DTTM desc) RNK
FROM
exp_pass_through_id
LEFT JOIN (
SELECT 
CNSUS_TRCT.CNSUS_TRCT_ID as CNSUS_TRCT_ID,
CNSUS_TRCT.CNTY_ID as CNTY_ID, CNSUS_TRCT.GEOGRCL_AREA_SBTYPE_CD as GEOGRCL_AREA_SBTYPE_CD, CNSUS_TRCT.LOCTR_SBTYPE_CD as LOCTR_SBTYPE_CD, CNSUS_TRCT.GEOGRCL_AREA_STRT_DT as GEOGRCL_AREA_STRT_DT, 
CNSUS_TRCT.EDW_STRT_DTTM as EDW_STRT_DTTM, CNSUS_TRCT.EDW_END_DTTM as EDW_END_DTTM, CNSUS_TRCT.GEOGRCL_AREA_SHRT_NAME as GEOGRCL_AREA_SHRT_NAME,
CNSUS_TRCT.GEOGRCL_AREA_NAME as GEOGRCL_AREA_NAME,
CNSUS_TRCT.GEOGRCL_AREA_DESC as GEOGRCL_AREA_DESC,
CNSUS_TRCT.CURY_CD as CURY_CD,
CNSUS_TRCT.GEOGRCL_AREA_END_DT as GEOGRCL_AREA_END_DT,
CNSUS_TRCT.PRCS_ID as PRCS_ID,

CNSUS_TRCT.CNSUS_TRCT_NUM as CNSUS_TRCT_NUM 
 FROM DB_T_PROD_CORE.CNSUS_TRCT
QUALIFY ROW_NUMBER() OVER(PARTITION BY CNTY_ID,GEOGRCL_AREA_SHRT_NAME,CNSUS_TRCT_NUM ORDER BY EDW_END_DTTM desc) = 1/*  */
) LKP ON LKP.CNTY_ID = exp_pass_through_id.o_cnty_id AND LKP.GEOGRCL_AREA_SHRT_NAME = exp_pass_through_id.County AND LKP.CNSUS_TRCT_NUM = exp_pass_through_id.o_TerritoryFIPSCode_alfa
QUALIFY RNK = 1
);


-- Component exp_SET_INS_UPD_FLAG, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_SET_INS_UPD_FLAG AS
(
SELECT
exp_pass_through_id.County as County,
''LOCTR_SBTYPE3'' as v_loctr_sbtype_val,
''GEOGRCL_AREA_SBTYPE3'' as v_geogrcl_sbtype_val,
LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_LOCTR_SBTYPE */ as v_loctr_sbtype,
LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_GEOGRCL_AREA_SBTYPE */ as v_geogrcl_sbtype,
v_loctr_sbtype as o_loctr_sbtype,
v_geogrcl_sbtype as o_geogrcl_sbtype,
:PRCS_ID as o_process_id,
exp_pass_through_id.CreateTime as CreateTime,
LKP_CNSUS_TRCT.CNSUS_TRCT_ID as lkp_CNSUS_TRCT_ID,
exp_pass_through_id.o_TerritoryFIPSCode_alfa as TerritoryFIPSCode_alfa,
MD5 ( LKP_CNSUS_TRCT.GEOGRCL_AREA_SBTYPE_CD || LKP_CNSUS_TRCT.LOCTR_SBTYPE_CD ) as lkp_checksum,
MD5 ( v_geogrcl_sbtype || v_loctr_sbtype ) as in_checksum,
CASE WHEN LKP_CNSUS_TRCT.CNSUS_TRCT_ID IS NULL THEN ''I'' ELSE ( CASE WHEN lkp_checksum <> in_checksum THEN ''U'' ELSE ''R'' END ) END as o_upd_or_ins,
CURRENT_TIMESTAMP as EDW_STRT_DTTM,
TO_TIMESTAMP ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as EDW_END_DTTM,
exp_pass_through_id.Retired as Retired,
exp_pass_through_id.o_cnty_id as in_cnty_id,
exp_pass_through_id.source_record_id,
row_number() over (partition by exp_pass_through_id.source_record_id order by exp_pass_through_id.source_record_id) as RNK1
FROM
exp_pass_through_id
INNER JOIN LKP_CNSUS_TRCT ON exp_pass_through_id.source_record_id = LKP_CNSUS_TRCT.source_record_id
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_LOCTR_SBTYPE LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = v_loctr_sbtype_val
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_GEOGRCL_AREA_SBTYPE LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = v_geogrcl_sbtype_val
QUALIFY RNK1 = 1
);


-- Component rtr_update_insert_CNSUS_INS, Type ROUTER Output Group INS
CREATE OR REPLACE TEMPORARY TABLE rtr_update_insert_CNSUS_INS AS
(SELECT
exp_SET_INS_UPD_FLAG.County as Short_Name,
exp_SET_INS_UPD_FLAG.County as Name,
exp_SET_INS_UPD_FLAG.o_upd_or_ins as UPD_OR_INS,
exp_SET_INS_UPD_FLAG.o_process_id as o_process_id,
exp_SET_INS_UPD_FLAG.o_loctr_sbtype as o_loctr_sbtype,
exp_SET_INS_UPD_FLAG.o_geogrcl_sbtype as o_geogrcl_sbtype,
exp_SET_INS_UPD_FLAG.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_SET_INS_UPD_FLAG.EDW_END_DTTM as EDW_END_DTTM,
exp_SET_INS_UPD_FLAG.CreateTime as CreateTime,
exp_SET_INS_UPD_FLAG.in_cnty_id as CNTY_ID,
exp_SET_INS_UPD_FLAG.Retired as Retired,
exp_SET_INS_UPD_FLAG.TerritoryFIPSCode_alfa as o_TerritoryFIPSCode_alfa,
exp_SET_INS_UPD_FLAG.lkp_CNSUS_TRCT_ID as lkp_CNSUS_TRCT_ID,
exp_SET_INS_UPD_FLAG.source_record_id
FROM
exp_SET_INS_UPD_FLAG
WHERE ( exp_SET_INS_UPD_FLAG.o_upd_or_ins = ''I'' ) or ( exp_SET_INS_UPD_FLAG.o_upd_or_ins = ''U'' ) 
-- OR ( exp_SET_INS_UPD_FLAG.o_upd_or_ins = ''U'' ) 
-- exp_SET_INS_UPD_FLAG.o_upd_or_ins = ''U'' AND lkp_EDW_END_DTTM = TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
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
rtr_update_insert_CNSUS_INS.CNTY_ID as CNTY_ID1,
rtr_update_insert_CNSUS_INS.o_TerritoryFIPSCode_alfa as o_TerritoryFIPSCode_alfa1,
rtr_update_insert_CNSUS_INS.lkp_CNSUS_TRCT_ID as lkp_CNSUS_TRCT_ID,
0 as UPDATE_STRATEGY_ACTION,
rtr_update_insert_CNSUS_INS.source_record_id
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
upd_insert_update_CNSUS.CNTY_ID1 as out_CNTY_ID,
upd_insert_update_CNSUS.o_loctr_sbtype1 as o_loctr_sbtype1,
upd_insert_update_CNSUS.o_geogrcl_sbtype1 as o_geogrcl_sbtype1,
upd_insert_update_CNSUS.EDW_STRT_DTTM1 as EDW_STRT_DTTM1,
upd_insert_update_CNSUS.EDW_END_DTTM1 as EDW_END_DTTM1,
upd_insert_update_CNSUS.CreateTime as CreateTime,
CASE WHEN upd_insert_update_CNSUS.lkp_CNSUS_TRCT_ID IS NULL THEN row_number() over (order by 1) ELSE upd_insert_update_CNSUS.lkp_CNSUS_TRCT_ID END as v_NEXTVAL,
v_NEXTVAL as O_NEXTVAL,
upd_insert_update_CNSUS.o_TerritoryFIPSCode_alfa1 as o_TerritoryFIPSCode_alfa1,
upd_insert_update_CNSUS.source_record_id
FROM
upd_insert_update_CNSUS
);


-- Component CNSUS_TRCT, Type TARGET 
INSERT INTO DB_T_PROD_CORE.CNSUS_TRCT
(
CNSUS_TRCT_ID,
CNTY_ID,
GEOGRCL_AREA_SBTYPE_CD,
GEOGRCL_AREA_SHRT_NAME,
GEOGRCL_AREA_NAME,
GEOGRCL_AREA_STRT_DT,
GEOGRCL_AREA_END_DT,
LOCTR_SBTYPE_CD,
CNSUS_TRCT_NUM,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM
)
SELECT
exp_pass_to_target_insert_pc111.O_NEXTVAL as CNSUS_TRCT_ID,
exp_pass_to_target_insert_pc111.out_CNTY_ID as CNTY_ID,
exp_pass_to_target_insert_pc111.o_geogrcl_sbtype1 as GEOGRCL_AREA_SBTYPE_CD,
exp_pass_to_target_insert_pc111.Short_Name1 as GEOGRCL_AREA_SHRT_NAME,
exp_pass_to_target_insert_pc111.Name1 as GEOGRCL_AREA_NAME,
exp_pass_to_target_insert_pc111.CreateTime as GEOGRCL_AREA_STRT_DT,
exp_pass_to_target_insert_pc111.EDW_END_DTTM1 as GEOGRCL_AREA_END_DT,
exp_pass_to_target_insert_pc111.o_loctr_sbtype1 as LOCTR_SBTYPE_CD,
exp_pass_to_target_insert_pc111.o_TerritoryFIPSCode_alfa1 as CNSUS_TRCT_NUM,
exp_pass_to_target_insert_pc111.o_process_id1 as PRCS_ID,
exp_pass_to_target_insert_pc111.EDW_STRT_DTTM1 as EDW_STRT_DTTM,
exp_pass_to_target_insert_pc111.EDW_END_DTTM1 as EDW_END_DTTM
FROM
exp_pass_to_target_insert_pc111;


-- PIPELINE END FOR 1

END; ';