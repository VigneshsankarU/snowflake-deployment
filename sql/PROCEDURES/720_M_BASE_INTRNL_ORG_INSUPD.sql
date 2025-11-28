-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_INTRNL_ORG_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 

declare
start_dttm timestamp;
end_dttm timestamp;
prcs_id int;
run_id string;


BEGIN 
run_id :=   (SELECT run_id   FROM control_run_id where upper(worklet_name) = upper(:worklet_name) order by insert_ts desc limit 1);   
END_DTTM:=   (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''END_DTTM'' order by insert_ts desc limit 1);
START_DTTM:=     (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''START_DTTM'' order by insert_ts desc limit 1);
PRCS_ID:=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PRCS_ID'' order by insert_ts desc limit 1);



-- Component LKP_TERADATA_ETL_REF_XLAT_INTRNL_ORG_SBTYPE, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_INTRNL_ORG_SBTYPE AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

		TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''INTRNL_ORG_SBTYPE''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_INTRNL_ORG_TYPE, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_INTRNL_ORG_TYPE AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''INTRNL_ORG_TYPE'' 

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

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''SRC_SYS'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_XREF_PRTY_INTRNL_ORG, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_XREF_PRTY_INTRNL_ORG AS
(
SELECT 
	DIR_PRTY.PRTY_ID as PRTY_ID, 
	DIR_PRTY.INTRNL_ORG_TYPE_CD as INTRNL_ORG_TYPE_CD, 
	DIR_PRTY.INTRNL_ORG_SBTYPE_CD as INTRNL_ORG_SBTYPE_CD, 
	DIR_PRTY.INTRNL_ORG_NUM as INTRNL_ORG_NUM, 
	DIR_PRTY.SRC_SYS_CD as SRC_SYS_CD 
FROM DB_T_PROD_CORE.DIR_PRTY
WHERE
	DIR_PRTY_VAL = ''INTRNL_ORG''
);


-- Component sq_pctl_uwcompanycode, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_pctl_uwcompanycode AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as TYPECODE,
$2 as NAME,
$3 as Type,
$4 as Subtype,
$5 as SYS_SRC_CD,
$6 as SRC_STRT_DT,
$7 as SRC_END_DT,
$8 as Retired,
$9 as Trans_strt_dttm,
$10 as Src,
$11 as LIC_IN_MULTI_ST_IND,
$12 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select key1,name1,type1,subtype,sys_src_cd,src_strt_dt,src_end_dt,

(case when retired=1 then 0 else retired end) as retired, /*  ADDED THIS LOGIC AS PART OF EIM-30376 COMMENTS TO REPLICATE EXISTING CONVERSION ISSUE */
trans_strt_dttm,src, LIC_IN_MULTI_ST_IND  from

(

SELECT	TYPECODE_stg AS Key1, 

name_stg as name1, 

CAST(''INTRNL_ORG_TYPE15'' AS VARCHAR(50)) AS Type1,

CAST( ''INTRNL_ORG_SBTYPE1''  AS VARCHAR ( 50)) AS Subtype, 

''SRC_SYS4'' AS SYS_SRC_CD,

TO_DATE(''19000101'',''YYYYMMDD'') AS SRC_STRT_DT,	

cast(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6)) AS SRC_END_DT,

cast(pctl_uwcompanycode.retired_stg as integer) as retired,

to_date(''1900-01-01'') Trans_strt_dttm,

cast (null as varchar(30)) as Src, 

cast (null as char(3)) as LIC_IN_MULTI_ST_IND



FROM	DB_T_PROD_STAG.pctl_uwcompanycode



UNION	



/*****Internal Organization(Sprint 11)*****/   

SELECT	Code_stg AS "Key", 

description_stg AS name, 

CAST(''INTRNL_ORG_TYPE15'' AS VARCHAR(50)) AS "Type",

CAST(''INTRNL_ORG_SBTYPE2'' AS VARCHAR(50)) AS "Subtype",

''SRC_SYS4'' AS SYS_SRC_CD,

createtime_stg AS SRC_STRT_DT,	

cast(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6)) AS SRC_END_DT,

cast(pc_producercode.retired_stg as integer) as retired,

cast(updatetime as date) Trans_strt_dttm,

cast (null as varchar(30)) as Src, 

(case when pc_producercode.IsLicensedInMultStates_alfa_stg = 0 then ''N''

 when pc_producercode.IsLicensedInMultStates_alfa_stg = 1 then ''Y''

 end) as LIC_IN_MULTI_ST_IND



FROM	( SELECT

pc_producercode.Code_stg,

pc_producercode.Description_stg,

 pc_producercode.CreateTime_stg,

CASE WHEN pc_producercode.UpdateTime_stg > usr.UpdateTime_stg then pc_producercode.UpdateTime_stg else usr.UpdateTime_stg end as UpdateTime,/* EIM-49001 */
 pc_producercode.retired_stg,

pc_producercode.ID_stg,

usr.IsLicensedInMultStates_alfa_stg

FROM

  DB_T_PROD_STAG.pc_producercode    join DB_T_PROD_STAG.pc_userproducercode upc on upc.ProducerCodeID_stg = pc_producercode.ID_stg

 join DB_T_PROD_STAG.pc_user usr on usr.id_stg = upc.UserID_stg

 join DB_T_PROD_STAG.pc_contact cnt on cnt.id_stg = usr.ContactID_stg  WHERE

(( pc_producercode.UpdateTime_stg > (:start_dttm) AND pc_producercode.UpdateTime_stg <= (:end_dttm))

OR (usr.updatetime_stg > (:start_dttm) AND usr.updatetime_stg <= (:end_dttm)))) pc_producercode /* EIM-49001 */
/*****Internal Organization(Sprint 11)*****/



/*  **** Sprint 12 changes start ****  */
UNION	

/****** DB_T_STAG_DM_PROD.Claims Hierarchy Groups**********/  

SELECT	cc_group.name_stg AS "Key", 

cc_group.name_stg as name,

CAST( ''INTRNL_ORG_TYPE15'' AS VARCHAR(50)) AS "Type",

cctl_grouptype.typecode_stg AS "Subtype",

''SRC_SYS6'' AS SYS_SRC_CD,	

case	when  cc_group.createtime_stg is null then TO_DATE(''19000101'',''YYYYMMDD'') 

else	 cc_group.createtime_stg 

end	 AS SRC_STRT_DT,

cast(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6)) AS SRC_END_DT,

cast(cc_group.retired_stg as integer) as retired,cc_group.updatetime_stg,

''cc_group'' as Src,

cast (null as char(3)) as LIC_IN_MULTI_ST_IND

FROM	(SELECT	cc_group.Retired_stg,

		cc_group.CreateTime_stg, cc_group.Name_stg,

		cc_group.UpdateTime_stg, cc_group.GroupType_stg

FROM

 DB_T_PROD_STAG.CC_GROUP

where	cc_group.UpdateTime_stg > (:start_dttm)

	and cc_group.UpdateTime_stg <= (:end_dttm)) CC_GROUP, DB_T_PROD_STAG.CCTL_GROUPTYPE

WHERE	CC_GROUP.GroupType_stg = CCTL_GROUPTYPE.ID_stg

/*************************************/

UNION	



/********Underwriter Hierarchy***********/

SELECT pc_group.name_stg AS "Key", 

pc_group.name_stg as name ,

CAST(''INTRNL_ORG_TYPE15'' AS VARCHAR(50)) AS "Type",

pctl_grouptype.typecode_stg AS "Subtype", 

''SRC_SYS4'' AS SYS_SRC_CD,

createtime_stg AS SRC_STRT_DT,	

cast(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6)) AS SRC_END_DT

,cast(pc_group.retired_stg as integer) as retired,

cast(updatetime_stg as date) Trans_strt_dttm,

cast (null as varchar(30)) as Src, 

cast (null as char(3)) as LIC_IN_MULTI_ST_IND



FROM	

(

SELECT

pc_group.Name_stg,

pc_group.CreateTime_stg,

pc_group.Retired_stg,

pc_group.UpdateTime_stg,

pc_group.GroupType_stg

FROM

 DB_T_PROD_STAG.pc_group 

WHERE 

pc_group.UpdateTime_stg > (:start_dttm)

and pc_group.UpdateTime_stg <= (:end_dttm)

) pc_group INNER JOIN DB_T_PROD_STAG.pctl_grouptype 

ON	pctl_grouptype.id_stg=pc_group.GroupType_stg

WHERE	

pctl_grouptype.TYPECODE_stg IN (''root'',''underwritingdistrict_alfa'',''homeofficeuw'')

/*************************************/

UNION	

/****************UW Region**************/  /*  New table */
SELECT	DISTINCT name_stg AS "Key", 

name_stg as name , CAST(''INTRNL_ORG_TYPE15'' AS VARCHAR(50)) AS "Type",

CAST(''INTRNL_ORG_SBTYPE3'' AS VARCHAR(50)) AS "Subtype", 

''SRC_SYS4'' AS SYS_SRC_CD,

createtime_stg  AS SRC_STRT_DT,	

cast(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6)) AS SRC_END_DT,

cast(pc_region.retired_stg as integer) as retired,

cast(updatetime_stg  as date) Trans_strt_dttm,

cast (null as varchar(30)) as Src, 

cast (null as char(3)) as LIC_IN_MULTI_ST_IND

FROM	( SELECT	pc_region.UpdateTime_stg,

		pc_region.CreateTime_stg, pc_region.Retired_stg,

		pc_region.Name_stg

FROM

 DB_T_PROD_STAG.pc_region

where

pc_region.UpdateTime_stg > (:start_dttm)

	and pc_region.UpdateTime_stg <= (:end_dttm)) pc_region 

qualify	row_number () over (partition by name 

order	by UPDATETIME_stg desc)=1

/**************************************/

UNION	

/****************UW Stae***************/ /*  New table  */
SELECT	DISTINCT pc_region_zone.code_stg AS "Key",

pc_region_zone.Code_stg AS name ,

CAST(''INTRNL_ORG_TYPE15'' AS VARCHAR(50)) AS "Type", 

CAST(''INTRNL_ORG_SBTYPE5'' AS VARCHAR(50)) AS "Subtype",

''SRC_SYS4'' AS SYS_SRC_CD, 

TO_DATE(''19000101'',''YYYYMMDD'') AS SRC_STRT_DT,

cast(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6)) AS SRC_END_DT,

cast (pctl_zonetype.retired_stg as integer) as retired,

cast(''01/01/1900'' as date) Trans_strt_dttm,

cast (null as varchar(30)) as Src, 

cast (null as char(3)) as LIC_IN_MULTI_ST_IND

FROM	

DB_T_PROD_STAG.pc_region_zone INNER JOIN DB_T_PROD_STAG.pctl_zonetype 

ON	pc_region_zone.ZoneType_stg=pctl_zonetype.id_stg

WHERE	pctl_zonetype.TYPECODE_stg=''state''

/*************************************/



UNION	



SELECT	

pc_group.name_stg AS "Key", 

pc_group.name_stg as name , 

CAST(''INTRNL_ORG_TYPE15'' AS VARCHAR(50)) AS "Type",

pctl_grouptype.typecode_stg AS "Subtype", ''SRC_SYS4'' AS SYS_SRC_CD,

createtime_stg  AS SRC_STRT_DT, cast(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6)) AS SRC_END_DT

,cast(pc_group.retired_stg as integer) as retired,cast(updatetime_stg  as date) Trans_strt_dttm,

cast (null as varchar(30)) as Src, 

cast (null as char(3)) as LIC_IN_MULTI_ST_IND

FROM	( SELECT

pc_group.Name_stg,

pc_group.CreateTime_stg,

pc_group.Retired_stg,

pc_group.UpdateTime_stg,

pc_group.GroupType_stg

FROM

 DB_T_PROD_STAG.pc_group 

WHERE 

pc_group.UpdateTime_stg > (:start_dttm)

and pc_group.UpdateTime_stg <= (:end_dttm)

) pc_group INNER JOIN DB_T_PROD_STAG.pctl_grouptype 

	ON	pctl_grouptype.id_stg=pc_group.GroupType_stg

WHERE	pctl_grouptype.TYPECODE_stg IN (''region'',''salesdistrict_alfa'',

		''servicecenter_alfa'')



UNION	



/***********Sales State****************/  /*  New table */
SELECT	DISTINCT pc_region_zone.code_stg AS "Key",

pc_region_zone.Code_stg AS name ,

CAST(''INTRNL_ORG_TYPE15'' AS VARCHAR(50)) AS "Type", 

CAST(''INTRNL_ORG_SBTYPE4'' AS VARCHAR(50)) AS "Subtype",

''SRC_SYS4'' AS SYS_SRC_CD, 

TO_DATE(''19000101'',''YYYYMMDD'') AS SRC_STRT_DT,

cast(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6)) AS SRC_END_DT,

cast (pctl_zonetype.retired_stg as integer) as retired,

cast(''01/01/1900'' as date) Trans_strt_dttm,

cast (null as varchar(30)) as Src, 

cast (null as char(3)) as LIC_IN_MULTI_ST_IND

FROM	

DB_T_PROD_STAG.pc_region_zone INNER JOIN DB_T_PROD_STAG.pctl_zonetype 

ON	pc_region_zone.ZoneType_stg=pctl_zonetype.id_stg

WHERE	pctl_zonetype.TYPECODE_stg=''state''

/*************************************/

)outr
) SRC
)
);


-- Component exp_pass_from_source, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_from_source AS
(
SELECT
sq_pctl_uwcompanycode.TYPECODE as in_TYPECODE,
UPPER ( sq_pctl_uwcompanycode.NAME ) as o_NAME,
LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_INTRNL_ORG_SBTYPE */ as INTRNL_ORG_SBTYPE_CD,
''UNK'' as in_ORG_TYPE_CODE,
''UNK'' as in_GICS_SBIDSTRY_CD,
''UNK'' as in_LIFCYCL_CD,
''UNK'' as in_PRTY_TYPE_CD,
LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SRC_CD */ as in_SYS_SRC_CD,
CURRENT_TIMESTAMP as EDW_STRT_DTTM,
TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as EDW_END_DTTM,
sq_pctl_uwcompanycode.SRC_STRT_DT as in_SRC_STRT_DT,
sq_pctl_uwcompanycode.SRC_END_DT as in_SRC_END_DT,
sq_pctl_uwcompanycode.Retired as Retired,
LKP_3.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_INTRNL_ORG_TYPE */ as INTERNAL_ORG_TYPE_CD,
CASE WHEN sq_pctl_uwcompanycode.Trans_strt_dttm IS NULL THEN to_date ( ''01/01/1900'' , ''mm/dd/yyyy'' ) ELSE sq_pctl_uwcompanycode.Trans_strt_dttm END as Trans_strt_dttm1,
sq_pctl_uwcompanycode.LIC_IN_MULTI_ST_IND as LIC_IN_MULTI_ST_IND,
sq_pctl_uwcompanycode.source_record_id,
row_number() over (partition by sq_pctl_uwcompanycode.source_record_id order by sq_pctl_uwcompanycode.source_record_id) as RNK
FROM
sq_pctl_uwcompanycode
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_INTRNL_ORG_SBTYPE LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = sq_pctl_uwcompanycode.Subtype
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_SRC_CD LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = sq_pctl_uwcompanycode.SYS_SRC_CD
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_INTRNL_ORG_TYPE LKP_3 ON LKP_3.SRC_IDNTFTN_VAL = sq_pctl_uwcompanycode.Type
QUALIFY RNK = 1
);


-- Component LKP_INTRNL_ORG, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_INTRNL_ORG AS
(
SELECT
LKP.INTRNL_ORG_PRTY_ID,
LKP.INTRNL_ORG_STRT_DTTM,
LKP.INTRNL_ORG_TYPE_CD,
LKP.INTRNL_ORG_SBTYPE_CD,
LKP.INTRNL_ORG_NUM,
LKP.ORG_TYPE_CD,
LKP.GICS_SBIDSTRY_CD,
LKP.PRTY_DESC,
LKP.INTRNL_ORG_END_DTTM,
LKP.LIFCYCL_CD,
LKP.PRTY_TYPE_CD,
LKP.EDW_STRT_DTTM,
LKP.EDW_END_DTTM,
LKP.SRC_SYS_CD,
LKP.LIC_IN_MULTI_ST_IND,
exp_pass_from_source.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_from_source.source_record_id ORDER BY LKP.INTRNL_ORG_PRTY_ID asc,LKP.INTRNL_ORG_STRT_DTTM asc,LKP.INTRNL_ORG_TYPE_CD asc,LKP.INTRNL_ORG_SBTYPE_CD asc,LKP.INTRNL_ORG_NUM asc,LKP.ORG_TYPE_CD asc,LKP.GICS_SBIDSTRY_CD asc,LKP.PRTY_DESC asc,LKP.INTRNL_ORG_END_DTTM asc,LKP.LIFCYCL_CD asc,LKP.PRTY_TYPE_CD asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc,LKP.SRC_SYS_CD asc,LKP.LIC_IN_MULTI_ST_IND asc) RNK
FROM
exp_pass_from_source
LEFT JOIN (
SELECT	INTRNL_ORG.INTRNL_ORG_PRTY_ID as INTRNL_ORG_PRTY_ID,
 INTRNL_ORG.INTRNL_ORG_STRT_DTTM as INTRNL_ORG_STRT_DTTM, 
INTRNL_ORG.INTRNL_ORG_TYPE_CD as INTRNL_ORG_TYPE_CD,
		INTRNL_ORG.INTRNL_ORG_SBTYPE_CD as INTRNL_ORG_SBTYPE_CD,
INTRNL_ORG.INTRNL_ORG_NUM as INTRNL_ORG_NUM,
INTRNL_ORG.ORG_TYPE_CD as ORG_TYPE_CD,
INTRNL_ORG.GICS_SBIDSTRY_CD as GICS_SBIDSTRY_CD,		
INTRNL_ORG.PRTY_DESC as PRTY_DESC, INTRNL_ORG.INTRNL_ORG_END_DTTM as INTRNL_ORG_END_DTTM,
INTRNL_ORG.LIFCYCL_CD as LIFCYCL_CD, INTRNL_ORG.PRTY_TYPE_CD as PRTY_TYPE_CD, 
INTRNL_ORG.EDW_STRT_DTTM AS EDW_STRT_DTTM,
INTRNL_ORG.EDW_END_DTTM as EDW_END_DTTM,INTRNL_ORG.SRC_SYS_CD as SRC_SYS_CD,
INTRNL_ORG.LIC_IN_MULTI_ST_IND as LIC_IN_MULTI_ST_IND
FROM	DB_T_PROD_CORE.INTRNL_ORG 
qualify	row_number () over (
partition by INTRNL_ORG_NUM,INTRNL_ORG_TYPE_CD,INTRNL_ORG_SBTYPE_CD,
		SRC_SYS_CD 
order by EDW_END_DTTM desc)=1
) LKP ON LKP.INTRNL_ORG_NUM = exp_pass_from_source.in_TYPECODE AND LKP.INTRNL_ORG_TYPE_CD = exp_pass_from_source.INTERNAL_ORG_TYPE_CD AND LKP.INTRNL_ORG_SBTYPE_CD = exp_pass_from_source.INTRNL_ORG_SBTYPE_CD AND LKP.SRC_SYS_CD = exp_pass_from_source.in_SYS_SRC_CD
QUALIFY RNK = 1
);


-- Component exp_compare_data, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_compare_data AS
(
SELECT
LKP_INTRNL_ORG.INTRNL_ORG_PRTY_ID as lkp_INTRNL_ORG_PRTY_ID,
LKP_INTRNL_ORG.INTRNL_ORG_TYPE_CD as lkp_INTRNL_ORG_TYPE_CD,
LKP_INTRNL_ORG.INTRNL_ORG_SBTYPE_CD as lkp_INTRNL_ORG_SBTYPE_CD,
LKP_INTRNL_ORG.INTRNL_ORG_NUM as lkp_INTRNL_ORG_NUM,
LKP_INTRNL_ORG.EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_pass_from_source.in_SRC_STRT_DT as in_SRC_STRT_DT,
exp_pass_from_source.in_SRC_END_DT as in_SRC_END_DT,
LKP_INTRNL_ORG.EDW_END_DTTM as lkp_EDW_END_DTTM,
LKP_INTRNL_ORG.SRC_SYS_CD as SRC_SYS_CD,
MD5 ( LTRIM ( RTRIM ( LKP_INTRNL_ORG.ORG_TYPE_CD ) ) || LTRIM ( RTRIM ( LKP_INTRNL_ORG.GICS_SBIDSTRY_CD ) ) || LTRIM ( RTRIM ( LKP_INTRNL_ORG.PRTY_DESC ) ) || LTRIM ( RTRIM ( LKP_INTRNL_ORG.LIFCYCL_CD ) ) || LTRIM ( RTRIM ( LKP_INTRNL_ORG.PRTY_TYPE_CD ) ) || ltrim ( rtrim ( to_char ( LKP_INTRNL_ORG.INTRNL_ORG_STRT_DTTM , ''yyyy-mm-dd'' ) ) ) || ltrim ( rtrim ( to_char ( LKP_INTRNL_ORG.INTRNL_ORG_END_DTTM , ''yyyy-mm-dd'' ) ) ) || ltrim ( rtrim ( LKP_INTRNL_ORG.LIC_IN_MULTI_ST_IND ) ) ) as v_lkp_checksum,
exp_pass_from_source.o_NAME as in_PRTY_DESC,
exp_pass_from_source.in_TYPECODE as in_TYPECODE,
exp_pass_from_source.INTERNAL_ORG_TYPE_CD as in_Type,
exp_pass_from_source.INTRNL_ORG_SBTYPE_CD as in_Subtype,
exp_pass_from_source.in_ORG_TYPE_CODE as in_ORG_TYPE_CODE,
exp_pass_from_source.in_GICS_SBIDSTRY_CD as in_GICS_SBIDSTRY_CD,
exp_pass_from_source.in_LIFCYCL_CD as in_LIFCYCL_CD,
exp_pass_from_source.in_PRTY_TYPE_CD as in_PRTY_TYPE_CD,
exp_pass_from_source.in_SYS_SRC_CD as in_SYS_SRC_CD,
exp_pass_from_source.LIC_IN_MULTI_ST_IND as in_LIC_IN_MULTI_ST_IND,
MD5 ( LTRIM ( RTRIM ( exp_pass_from_source.in_ORG_TYPE_CODE ) ) || LTRIM ( RTRIM ( exp_pass_from_source.in_GICS_SBIDSTRY_CD ) ) || LTRIM ( RTRIM ( exp_pass_from_source.o_NAME ) ) || LTRIM ( RTRIM ( exp_pass_from_source.in_LIFCYCL_CD ) ) || LTRIM ( RTRIM ( exp_pass_from_source.in_PRTY_TYPE_CD ) ) || ltrim ( rtrim ( to_char ( exp_pass_from_source.in_SRC_STRT_DT , ''yyyy-mm-dd'' ) ) ) || ltrim ( rtrim ( to_char ( exp_pass_from_source.in_SRC_END_DT , ''yyyy-mm-dd'' ) ) ) || ltrim ( rtrim ( exp_pass_from_source.LIC_IN_MULTI_ST_IND ) ) ) as v_in_checksum,
exp_pass_from_source.EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_pass_from_source.EDW_END_DTTM as in_EDW_END_DTTM,
CASE WHEN v_lkp_checksum IS NULL THEN ''I'' ELSE CASE WHEN v_lkp_checksum != v_in_checksum THEN ''U'' ELSE ''R'' END END as calc_ins_upd,
:PRCS_ID as PRCS_ID,
LKP_1.PRTY_ID /* replaced lookup LKP_XREF_PRTY_INTRNL_ORG */ as v_INTRNL_ORG_PRTY_ID,
v_INTRNL_ORG_PRTY_ID as in_INTRNL_ORG_PRTY_ID,
exp_pass_from_source.Retired as Retired,
NULL as LKP_TRANS_STRT_DTTM,
sq_pctl_uwcompanycode.Src as Src,
exp_pass_from_source.Trans_strt_dttm1 as Trans_strt_dttm1,
CURRENT_TIMESTAMP as trans_SYSDATE,
sq_pctl_uwcompanycode.source_record_id,
row_number() over (partition by sq_pctl_uwcompanycode.source_record_id order by sq_pctl_uwcompanycode.source_record_id) as RNK1
FROM
sq_pctl_uwcompanycode
INNER JOIN exp_pass_from_source ON sq_pctl_uwcompanycode.source_record_id = exp_pass_from_source.source_record_id
INNER JOIN LKP_INTRNL_ORG ON exp_pass_from_source.source_record_id = LKP_INTRNL_ORG.source_record_id
LEFT JOIN LKP_XREF_PRTY_INTRNL_ORG LKP_1 ON LKP_1.INTRNL_ORG_TYPE_CD = ltrim ( rtrim ( exp_pass_from_source.INTERNAL_ORG_TYPE_CD ) ) AND LKP_1.INTRNL_ORG_SBTYPE_CD = ltrim ( rtrim ( exp_pass_from_source.INTRNL_ORG_SBTYPE_CD ) ) AND LKP_1.INTRNL_ORG_NUM = ltrim ( rtrim ( exp_pass_from_source.in_TYPECODE ) ) AND LKP_1.SRC_SYS_CD = ltrim ( rtrim ( exp_pass_from_source.in_SYS_SRC_CD ) )
QUALIFY RNK1 = 1
);


-- Component rtr_intrnl_org_INSERT, Type ROUTER Output Group INSERT
CREATE OR REPLACE TEMPORARY TABLE rtr_intrnl_org_INSERT AS
(SELECT
exp_compare_data.lkp_INTRNL_ORG_PRTY_ID as lkp_INTRNL_ORG_PRTY_ID,
exp_compare_data.lkp_INTRNL_ORG_TYPE_CD as lkp_INTRNL_ORG_TYPE_CD,
exp_compare_data.lkp_INTRNL_ORG_SBTYPE_CD as lkp_INTRNL_ORG_SBTYPE_CD,
exp_compare_data.lkp_INTRNL_ORG_NUM as lkp_INTRNL_ORG_NUM,
exp_compare_data.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_compare_data.SRC_SYS_CD as SRC_SYS_CD,
exp_compare_data.in_INTRNL_ORG_PRTY_ID as in_INTRNL_ORG_PRTY_ID,
exp_compare_data.in_PRTY_DESC as in_PRTY_DESC,
exp_compare_data.in_SYS_SRC_CD as in_SYS_SRC_CD,
exp_compare_data.in_ORG_TYPE_CODE as in_ORG_TYPE_CODE,
exp_compare_data.in_GICS_SBIDSTRY_CD as in_GICS_SBIDSTRY_CD,
exp_compare_data.in_LIFCYCL_CD as in_LIFCYCL_CD,
exp_compare_data.in_PRTY_TYPE_CD as in_PRTY_TYPE_CD,
exp_compare_data.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_compare_data.in_EDW_END_DTTM as in_EDW_END_DTTM,
exp_compare_data.in_LIC_IN_MULTI_ST_IND as in_LIC_IN_MULTI_ST_IND,
exp_compare_data.calc_ins_upd as calc_ins_upd,
exp_compare_data.PRCS_ID as PRCS_ID,
exp_compare_data.in_TYPECODE as in_TYPECODE,
exp_compare_data.in_Type as in_Type,
exp_compare_data.in_Subtype as in_Subtype,
exp_compare_data.in_SRC_STRT_DT as in_SRC_STRT_DT,
exp_compare_data.in_SRC_END_DT as in_SRC_END_DT,
exp_compare_data.Retired as Retired,
exp_compare_data.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_compare_data.Trans_strt_dttm1 as Trans_strt_dttm1,
exp_compare_data.LKP_TRANS_STRT_DTTM as LKP_TRANS_STRT_DTTM,
exp_compare_data.Src as Src,
exp_compare_data.trans_SYSDATE as trans_SYSDATE,
exp_compare_data.source_record_id
FROM
exp_compare_data
WHERE exp_compare_data.calc_ins_upd = ''I'' OR ( exp_compare_data.Retired = 0 and exp_compare_data.lkp_EDW_END_DTTM != TO_timestamp ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ));


-- Component rtr_intrnl_org_RETIRE, Type ROUTER Output Group RETIRE
CREATE OR REPLACE TEMPORARY TABLE rtr_intrnl_org_RETIRE AS
(SELECT
exp_compare_data.lkp_INTRNL_ORG_PRTY_ID as lkp_INTRNL_ORG_PRTY_ID,
exp_compare_data.lkp_INTRNL_ORG_TYPE_CD as lkp_INTRNL_ORG_TYPE_CD,
exp_compare_data.lkp_INTRNL_ORG_SBTYPE_CD as lkp_INTRNL_ORG_SBTYPE_CD,
exp_compare_data.lkp_INTRNL_ORG_NUM as lkp_INTRNL_ORG_NUM,
exp_compare_data.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_compare_data.SRC_SYS_CD as SRC_SYS_CD,
exp_compare_data.in_INTRNL_ORG_PRTY_ID as in_INTRNL_ORG_PRTY_ID,
exp_compare_data.in_PRTY_DESC as in_PRTY_DESC,
exp_compare_data.in_SYS_SRC_CD as in_SYS_SRC_CD,
exp_compare_data.in_ORG_TYPE_CODE as in_ORG_TYPE_CODE,
exp_compare_data.in_GICS_SBIDSTRY_CD as in_GICS_SBIDSTRY_CD,
exp_compare_data.in_LIFCYCL_CD as in_LIFCYCL_CD,
exp_compare_data.in_PRTY_TYPE_CD as in_PRTY_TYPE_CD,
exp_compare_data.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_compare_data.in_EDW_END_DTTM as in_EDW_END_DTTM,
exp_compare_data.in_LIC_IN_MULTI_ST_IND as in_LIC_IN_MULTI_ST_IND,
exp_compare_data.calc_ins_upd as calc_ins_upd,
exp_compare_data.PRCS_ID as PRCS_ID,
exp_compare_data.in_TYPECODE as in_TYPECODE,
exp_compare_data.in_Type as in_Type,
exp_compare_data.in_Subtype as in_Subtype,
exp_compare_data.in_SRC_STRT_DT as in_SRC_STRT_DT,
exp_compare_data.in_SRC_END_DT as in_SRC_END_DT,
exp_compare_data.Retired as Retired,
exp_compare_data.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_compare_data.Trans_strt_dttm1 as Trans_strt_dttm1,
exp_compare_data.LKP_TRANS_STRT_DTTM as LKP_TRANS_STRT_DTTM,
exp_compare_data.Src as Src,
exp_compare_data.trans_SYSDATE as trans_SYSDATE,
exp_compare_data.source_record_id
FROM
exp_compare_data
WHERE exp_compare_data.calc_ins_upd = ''R'' and exp_compare_data.Retired != 0 and exp_compare_data.lkp_EDW_END_DTTM = TO_timestamp ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ));


-- Component rtr_intrnl_org_UPDATE, Type ROUTER Output Group UPDATE
CREATE OR REPLACE TEMPORARY TABLE rtr_intrnl_org_UPDATE AS
(SELECT
exp_compare_data.lkp_INTRNL_ORG_PRTY_ID as lkp_INTRNL_ORG_PRTY_ID,
exp_compare_data.lkp_INTRNL_ORG_TYPE_CD as lkp_INTRNL_ORG_TYPE_CD,
exp_compare_data.lkp_INTRNL_ORG_SBTYPE_CD as lkp_INTRNL_ORG_SBTYPE_CD,
exp_compare_data.lkp_INTRNL_ORG_NUM as lkp_INTRNL_ORG_NUM,
exp_compare_data.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_compare_data.SRC_SYS_CD as SRC_SYS_CD,
exp_compare_data.in_INTRNL_ORG_PRTY_ID as in_INTRNL_ORG_PRTY_ID,
exp_compare_data.in_PRTY_DESC as in_PRTY_DESC,
exp_compare_data.in_SYS_SRC_CD as in_SYS_SRC_CD,
exp_compare_data.in_ORG_TYPE_CODE as in_ORG_TYPE_CODE,
exp_compare_data.in_GICS_SBIDSTRY_CD as in_GICS_SBIDSTRY_CD,
exp_compare_data.in_LIFCYCL_CD as in_LIFCYCL_CD,
exp_compare_data.in_PRTY_TYPE_CD as in_PRTY_TYPE_CD,
exp_compare_data.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_compare_data.in_EDW_END_DTTM as in_EDW_END_DTTM,
exp_compare_data.in_LIC_IN_MULTI_ST_IND as in_LIC_IN_MULTI_ST_IND,
exp_compare_data.calc_ins_upd as calc_ins_upd,
exp_compare_data.PRCS_ID as PRCS_ID,
exp_compare_data.in_TYPECODE as in_TYPECODE,
exp_compare_data.in_Type as in_Type,
exp_compare_data.in_Subtype as in_Subtype,
exp_compare_data.in_SRC_STRT_DT as in_SRC_STRT_DT,
exp_compare_data.in_SRC_END_DT as in_SRC_END_DT,
exp_compare_data.Retired as Retired,
exp_compare_data.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_compare_data.Trans_strt_dttm1 as Trans_strt_dttm1,
exp_compare_data.LKP_TRANS_STRT_DTTM as LKP_TRANS_STRT_DTTM,
exp_compare_data.Src as Src,
exp_compare_data.trans_SYSDATE as trans_SYSDATE,
exp_compare_data.source_record_id
FROM
exp_compare_data
WHERE exp_compare_data.calc_ins_upd = ''U'' AND exp_compare_data.lkp_EDW_END_DTTM = TO_timestamp ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ));


-- Component upd_intrnl_org_upd, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_intrnl_org_upd AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_intrnl_org_UPDATE.lkp_INTRNL_ORG_PRTY_ID as lkp_INTRNL_ORG_PRTY_ID3,
rtr_intrnl_org_UPDATE.lkp_INTRNL_ORG_TYPE_CD as lkp_INTRNL_ORG_TYPE_CD3,
rtr_intrnl_org_UPDATE.lkp_INTRNL_ORG_SBTYPE_CD as lkp_INTRNL_ORG_SBTYPE_CD3,
rtr_intrnl_org_UPDATE.lkp_INTRNL_ORG_NUM as lkp_INTRNL_ORG_NUM3,
rtr_intrnl_org_UPDATE.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM3,
rtr_intrnl_org_UPDATE.SRC_SYS_CD as SRC_SYS_CD,
rtr_intrnl_org_UPDATE.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM3,
rtr_intrnl_org_UPDATE.PRCS_ID as PRCS_ID3,
rtr_intrnl_org_UPDATE.Trans_strt_dttm1 as TRANS_STRT_DTTM3,
rtr_intrnl_org_UPDATE.Src as Src3,
rtr_intrnl_org_UPDATE.Retired as Retired3,
rtr_intrnl_org_UPDATE.trans_SYSDATE as trans_SYSDATE,
1 as UPDATE_STRATEGY_ACTION,
rtr_intrnl_org_UPDATE.source_record_id
FROM
rtr_intrnl_org_UPDATE
);


-- Component upd_intrnl_org_upd1, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_intrnl_org_upd1 AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_intrnl_org_RETIRE.lkp_INTRNL_ORG_PRTY_ID as lkp_INTRNL_ORG_PRTY_ID3,
rtr_intrnl_org_RETIRE.lkp_INTRNL_ORG_TYPE_CD as lkp_INTRNL_ORG_TYPE_CD3,
rtr_intrnl_org_RETIRE.lkp_INTRNL_ORG_SBTYPE_CD as lkp_INTRNL_ORG_SBTYPE_CD3,
rtr_intrnl_org_RETIRE.lkp_INTRNL_ORG_NUM as lkp_INTRNL_ORG_NUM3,
rtr_intrnl_org_RETIRE.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM3,
rtr_intrnl_org_RETIRE.SRC_SYS_CD as SRC_SYS_CD,
rtr_intrnl_org_RETIRE.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM3,
rtr_intrnl_org_RETIRE.PRCS_ID as PRCS_ID3,
rtr_intrnl_org_RETIRE.Trans_strt_dttm1 as Trans_strt_dttm14,
1 as UPDATE_STRATEGY_ACTION,
rtr_intrnl_org_RETIRE.source_record_id
FROM
rtr_intrnl_org_RETIRE
);


-- Component upd_intrnl_org_insert, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_intrnl_org_insert AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_intrnl_org_INSERT.in_INTRNL_ORG_PRTY_ID as in_INTRNL_ORG_PRTY_ID1,
rtr_intrnl_org_INSERT.in_PRTY_DESC as in_PRTY_DESC1,
rtr_intrnl_org_INSERT.in_SYS_SRC_CD as in_SYS_SRC_CD1,
rtr_intrnl_org_INSERT.in_ORG_TYPE_CODE as in_ORG_TYPE_CODE1,
rtr_intrnl_org_INSERT.in_GICS_SBIDSTRY_CD as in_GICS_SBIDSTRY_CD1,
rtr_intrnl_org_INSERT.in_LIFCYCL_CD as in_LIFCYCL_CD1,
rtr_intrnl_org_INSERT.in_PRTY_TYPE_CD as in_PRTY_TYPE_CD1,
rtr_intrnl_org_INSERT.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM1,
rtr_intrnl_org_INSERT.in_EDW_END_DTTM as in_EDW_END_DTTM1,
rtr_intrnl_org_INSERT.in_LIC_IN_MULTI_ST_IND as in_LIC_IN_MULTI_ST_IND1,
rtr_intrnl_org_INSERT.PRCS_ID as PRCS_ID1,
rtr_intrnl_org_INSERT.in_TYPECODE as in_TYPECODE1,
rtr_intrnl_org_INSERT.in_Type as in_Type1,
rtr_intrnl_org_INSERT.in_Subtype as in_Subtype1,
rtr_intrnl_org_INSERT.in_SRC_STRT_DT as in_SRC_STRT_DT1,
rtr_intrnl_org_INSERT.in_SRC_END_DT as in_SRC_END_DT1,
rtr_intrnl_org_INSERT.Retired as Retired1,
rtr_intrnl_org_INSERT.Trans_strt_dttm1 as Trans_strt_dttm11,
0 as UPDATE_STRATEGY_ACTION,
rtr_intrnl_org_INSERT.source_record_id
FROM
rtr_intrnl_org_INSERT
);


-- Component upd_intrnl_org_upd_ins, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_intrnl_org_upd_ins AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_intrnl_org_UPDATE.lkp_INTRNL_ORG_PRTY_ID as lkp_INTRNL_ORG_PRTY_ID3,
rtr_intrnl_org_UPDATE.lkp_INTRNL_ORG_TYPE_CD as lkp_INTRNL_ORG_TYPE_CD3,
rtr_intrnl_org_UPDATE.lkp_INTRNL_ORG_SBTYPE_CD as lkp_INTRNL_ORG_SBTYPE_CD3,
rtr_intrnl_org_UPDATE.lkp_INTRNL_ORG_NUM as lkp_INTRNL_ORG_NUM3,
rtr_intrnl_org_UPDATE.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM3,
rtr_intrnl_org_UPDATE.SRC_SYS_CD as lkp_SYS_SRC_CD3,
rtr_intrnl_org_UPDATE.in_PRTY_DESC as in_PRTY_DESC3,
rtr_intrnl_org_UPDATE.in_ORG_TYPE_CODE as in_ORG_TYPE_CODE3,
rtr_intrnl_org_UPDATE.in_GICS_SBIDSTRY_CD as in_GICS_SBIDSTRY_CD3,
rtr_intrnl_org_UPDATE.in_LIFCYCL_CD as in_LIFCYCL_CD3,
rtr_intrnl_org_UPDATE.in_PRTY_TYPE_CD as in_PRTY_TYPE_CD3,
rtr_intrnl_org_UPDATE.in_EDW_END_DTTM as in_EDW_END_DTTM3,
rtr_intrnl_org_UPDATE.in_LIC_IN_MULTI_ST_IND as in_LIC_IN_MULTI_ST_IND3,
rtr_intrnl_org_UPDATE.PRCS_ID as PRCS_ID3,
rtr_intrnl_org_UPDATE.in_SRC_STRT_DT as in_SRC_STRT_DT1,
rtr_intrnl_org_UPDATE.in_SRC_END_DT as in_SRC_END_DT1,
rtr_intrnl_org_UPDATE.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM3,
rtr_intrnl_org_UPDATE.Retired as Retired3,
rtr_intrnl_org_UPDATE.Trans_strt_dttm1 as Trans_strt_dttm13,
rtr_intrnl_org_UPDATE.Src as Src3,
rtr_intrnl_org_UPDATE.trans_SYSDATE as trans_SYSDATE,
0 as UPDATE_STRATEGY_ACTION,
rtr_intrnl_org_UPDATE.source_record_id
FROM
rtr_intrnl_org_UPDATE
);


-- Component fil_active_recs, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE fil_active_recs AS
(
SELECT
upd_intrnl_org_upd_ins.lkp_INTRNL_ORG_PRTY_ID3 as lkp_INTRNL_ORG_PRTY_ID3,
upd_intrnl_org_upd_ins.lkp_INTRNL_ORG_TYPE_CD3 as lkp_INTRNL_ORG_TYPE_CD3,
upd_intrnl_org_upd_ins.lkp_INTRNL_ORG_SBTYPE_CD3 as lkp_INTRNL_ORG_SBTYPE_CD3,
upd_intrnl_org_upd_ins.lkp_INTRNL_ORG_NUM3 as lkp_INTRNL_ORG_NUM3,
upd_intrnl_org_upd_ins.lkp_EDW_STRT_DTTM3 as lkp_EDW_STRT_DTTM3,
upd_intrnl_org_upd_ins.lkp_SYS_SRC_CD3 as lkp_SYS_SRC_CD3,
upd_intrnl_org_upd_ins.in_PRTY_DESC3 as in_PRTY_DESC3,
upd_intrnl_org_upd_ins.in_ORG_TYPE_CODE3 as in_ORG_TYPE_CODE3,
upd_intrnl_org_upd_ins.in_GICS_SBIDSTRY_CD3 as in_GICS_SBIDSTRY_CD3,
upd_intrnl_org_upd_ins.in_LIFCYCL_CD3 as in_LIFCYCL_CD3,
upd_intrnl_org_upd_ins.in_PRTY_TYPE_CD3 as in_PRTY_TYPE_CD3,
upd_intrnl_org_upd_ins.in_EDW_END_DTTM3 as in_EDW_END_DTTM3,
upd_intrnl_org_upd_ins.in_LIC_IN_MULTI_ST_IND3 as in_LIC_IN_MULTI_ST_IND3,
upd_intrnl_org_upd_ins.PRCS_ID3 as PRCS_ID3,
upd_intrnl_org_upd_ins.in_SRC_STRT_DT1 as in_SRC_STRT_DT1,
upd_intrnl_org_upd_ins.in_SRC_END_DT1 as in_SRC_END_DT1,
upd_intrnl_org_upd_ins.in_EDW_STRT_DTTM3 as in_EDW_STRT_DTTM3,
upd_intrnl_org_upd_ins.Retired3 as Retired3,
upd_intrnl_org_upd_ins.Trans_strt_dttm13 as Trans_strt_dttm13,
upd_intrnl_org_upd_ins.Src3 as Src3,
upd_intrnl_org_upd_ins.trans_SYSDATE as trans_SYSDATE,
upd_intrnl_org_upd_ins.source_record_id
FROM
upd_intrnl_org_upd_ins
WHERE upd_intrnl_org_upd_ins.Retired3 = 0
);


-- Component exp_intrnl_org_upd, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_intrnl_org_upd AS
(
SELECT
upd_intrnl_org_upd.lkp_INTRNL_ORG_PRTY_ID3 as lkp_INTRNL_ORG_PRTY_ID3,
upd_intrnl_org_upd.lkp_INTRNL_ORG_TYPE_CD3 as lkp_INTRNL_ORG_TYPE_CD3,
upd_intrnl_org_upd.lkp_INTRNL_ORG_SBTYPE_CD3 as lkp_INTRNL_ORG_SBTYPE_CD3,
upd_intrnl_org_upd.lkp_INTRNL_ORG_NUM3 as lkp_INTRNL_ORG_NUM3,
upd_intrnl_org_upd.lkp_EDW_STRT_DTTM3 as lkp_EDW_STRT_DTTM3,
upd_intrnl_org_upd.SRC_SYS_CD as SRC_SYS_CD,
DATEADD (SECOND, -1, upd_intrnl_org_upd.in_EDW_STRT_DTTM3) as EDW_END_DTTM,
CASE
  WHEN upd_intrnl_org_upd.Retired3 != 0 THEN CURRENT_TIMESTAMP()
  ELSE CASE
    WHEN upd_intrnl_org_upd.TRANS_STRT_DTTM3 = TO_TIMESTAMP (''1900-01-01 00:00:00.000000'') THEN DATEADD (SECOND, -1, upd_intrnl_org_upd.trans_SYSDATE)
    ELSE DATEADD (SECOND, -1, upd_intrnl_org_upd.TRANS_STRT_DTTM3)
  END
END as TRANS_END_DTTM,
upd_intrnl_org_upd.source_record_id
FROM
upd_intrnl_org_upd
);


-- Component exp_intrnl_org_upd1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_intrnl_org_upd1 AS
(
SELECT
upd_intrnl_org_upd1.lkp_INTRNL_ORG_PRTY_ID3 as lkp_INTRNL_ORG_PRTY_ID3,
upd_intrnl_org_upd1.lkp_INTRNL_ORG_TYPE_CD3 as lkp_INTRNL_ORG_TYPE_CD3,
upd_intrnl_org_upd1.lkp_INTRNL_ORG_SBTYPE_CD3 as lkp_INTRNL_ORG_SBTYPE_CD3,
upd_intrnl_org_upd1.lkp_INTRNL_ORG_NUM3 as lkp_INTRNL_ORG_NUM3,
upd_intrnl_org_upd1.lkp_EDW_STRT_DTTM3 as lkp_EDW_STRT_DTTM3,
upd_intrnl_org_upd1.SRC_SYS_CD as SRC_SYS_CD,
CURRENT_TIMESTAMP as EDW_END_DTTM,
upd_intrnl_org_upd1.Trans_strt_dttm14 as Trans_strt_dttm14,
upd_intrnl_org_upd1.source_record_id
FROM
upd_intrnl_org_upd1
);


-- Component tgt_intrnl_org_upd, Type TARGET 
MERGE INTO DB_T_PROD_CORE.INTRNL_ORG
USING exp_intrnl_org_upd ON (INTRNL_ORG.INTRNL_ORG_PRTY_ID = exp_intrnl_org_upd.lkp_INTRNL_ORG_PRTY_ID3 AND INTRNL_ORG.EDW_STRT_DTTM = exp_intrnl_org_upd.lkp_EDW_STRT_DTTM3)
WHEN MATCHED THEN UPDATE
SET
INTRNL_ORG_PRTY_ID = exp_intrnl_org_upd.lkp_INTRNL_ORG_PRTY_ID3,
INTRNL_ORG_TYPE_CD = exp_intrnl_org_upd.lkp_INTRNL_ORG_TYPE_CD3,
INTRNL_ORG_SBTYPE_CD = exp_intrnl_org_upd.lkp_INTRNL_ORG_SBTYPE_CD3,
INTRNL_ORG_NUM = exp_intrnl_org_upd.lkp_INTRNL_ORG_NUM3,
SRC_SYS_CD = exp_intrnl_org_upd.SRC_SYS_CD,
EDW_STRT_DTTM = exp_intrnl_org_upd.lkp_EDW_STRT_DTTM3,
EDW_END_DTTM = exp_intrnl_org_upd.EDW_END_DTTM,
TRANS_END_DTTM = exp_intrnl_org_upd.TRANS_END_DTTM;


-- Component exp_intrnl_org_insert, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_intrnl_org_insert AS
(
SELECT
upd_intrnl_org_insert.in_INTRNL_ORG_PRTY_ID1 as in_INTRNL_ORG_PRTY_ID1,
upd_intrnl_org_insert.in_PRTY_DESC1 as in_PRTY_DESC1,
upd_intrnl_org_insert.in_SYS_SRC_CD1 as in_SYS_SRC_CD1,
upd_intrnl_org_insert.in_ORG_TYPE_CODE1 as in_ORG_TYPE_CODE1,
upd_intrnl_org_insert.in_GICS_SBIDSTRY_CD1 as in_GICS_SBIDSTRY_CD1,
upd_intrnl_org_insert.in_LIFCYCL_CD1 as in_LIFCYCL_CD1,
upd_intrnl_org_insert.in_PRTY_TYPE_CD1 as in_PRTY_TYPE_CD1,
upd_intrnl_org_insert.in_EDW_STRT_DTTM1 as in_EDW_STRT_DTTM1,
CASE WHEN upd_intrnl_org_insert.Retired1 = 0 THEN upd_intrnl_org_insert.in_EDW_END_DTTM1 ELSE CURRENT_TIMESTAMP END as out_EDW_END_DTTM11,
upd_intrnl_org_insert.in_LIC_IN_MULTI_ST_IND1 as in_LIC_IN_MULTI_ST_IND1,
upd_intrnl_org_insert.PRCS_ID1 as PRCS_ID1,
upd_intrnl_org_insert.in_TYPECODE1 as in_TYPECODE1,
upd_intrnl_org_insert.in_Type1 as in_Type1,
upd_intrnl_org_insert.in_Subtype1 as in_Subtype1,
upd_intrnl_org_insert.in_SRC_STRT_DT1 as in_SRC_STRT_DT1,
upd_intrnl_org_insert.in_SRC_END_DT1 as in_SRC_END_DT1,
upd_intrnl_org_insert.Trans_strt_dttm11 as Trans_strt_dttm11,
CASE WHEN upd_intrnl_org_insert.Retired1 != 0 THEN upd_intrnl_org_insert.Trans_strt_dttm11 ELSE to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) END as TRANS_END_DTTM,
upd_intrnl_org_insert.source_record_id
FROM
upd_intrnl_org_insert
);


-- Component tgt_intrnl_org_retire, Type TARGET 
MERGE INTO DB_T_PROD_CORE.INTRNL_ORG
USING exp_intrnl_org_upd1 ON (INTRNL_ORG.INTRNL_ORG_PRTY_ID = exp_intrnl_org_upd1.lkp_INTRNL_ORG_PRTY_ID3 AND INTRNL_ORG.EDW_STRT_DTTM = exp_intrnl_org_upd1.lkp_EDW_STRT_DTTM3)
WHEN MATCHED THEN UPDATE
SET
INTRNL_ORG_PRTY_ID = exp_intrnl_org_upd1.lkp_INTRNL_ORG_PRTY_ID3,
INTRNL_ORG_TYPE_CD = exp_intrnl_org_upd1.lkp_INTRNL_ORG_TYPE_CD3,
INTRNL_ORG_SBTYPE_CD = exp_intrnl_org_upd1.lkp_INTRNL_ORG_SBTYPE_CD3,
INTRNL_ORG_NUM = exp_intrnl_org_upd1.lkp_INTRNL_ORG_NUM3,
SRC_SYS_CD = exp_intrnl_org_upd1.SRC_SYS_CD,
EDW_STRT_DTTM = exp_intrnl_org_upd1.lkp_EDW_STRT_DTTM3,
EDW_END_DTTM = exp_intrnl_org_upd1.EDW_END_DTTM,
TRANS_END_DTTM = exp_intrnl_org_upd1.Trans_strt_dttm14;


-- Component exp_intrnl_org_upd_ins, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_intrnl_org_upd_ins AS
(
SELECT
fil_active_recs.lkp_INTRNL_ORG_PRTY_ID3 as lkp_INTRNL_ORG_PRTY_ID3,
fil_active_recs.lkp_INTRNL_ORG_TYPE_CD3 as lkp_INTRNL_ORG_TYPE_CD3,
fil_active_recs.lkp_INTRNL_ORG_SBTYPE_CD3 as lkp_INTRNL_ORG_SBTYPE_CD3,
fil_active_recs.lkp_INTRNL_ORG_NUM3 as lkp_INTRNL_ORG_NUM3,
fil_active_recs.lkp_SYS_SRC_CD3 as lkp_SYS_SRC_CD3,
fil_active_recs.in_PRTY_DESC3 as in_PRTY_DESC3,
fil_active_recs.in_ORG_TYPE_CODE3 as in_ORG_TYPE_CODE3,
fil_active_recs.in_GICS_SBIDSTRY_CD3 as in_GICS_SBIDSTRY_CD3,
fil_active_recs.in_LIFCYCL_CD3 as in_LIFCYCL_CD3,
fil_active_recs.in_PRTY_TYPE_CD3 as in_PRTY_TYPE_CD3,
fil_active_recs.in_EDW_END_DTTM3 as in_EDW_END_DTTM3,
fil_active_recs.in_LIC_IN_MULTI_ST_IND3 as in_LIC_IN_MULTI_ST_IND3,
fil_active_recs.PRCS_ID3 as PRCS_ID3,
fil_active_recs.in_SRC_STRT_DT1 as in_SRC_STRT_DT1,
fil_active_recs.in_SRC_END_DT1 as in_SRC_END_DT1,
fil_active_recs.in_EDW_STRT_DTTM3 as in_EDW_STRT_DTTM3,
CASE WHEN fil_active_recs.Trans_strt_dttm13 = TO_timestamp ( ''1900-01-01 00:00:00.000000'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) THEN fil_active_recs.trans_SYSDATE ELSE fil_active_recs.Trans_strt_dttm13 END as out_TRANS_STRT_DTTM,
fil_active_recs.source_record_id
FROM
fil_active_recs
);


-- Component tgt_intrnl_org_ins, Type TARGET 
INSERT INTO DB_T_PROD_CORE.INTRNL_ORG
(
INTRNL_ORG_PRTY_ID,
INTRNL_ORG_STRT_DTTM,
INTRNL_ORG_TYPE_CD,
INTRNL_ORG_SBTYPE_CD,
INTRNL_ORG_NUM,
ORG_TYPE_CD,
GICS_SBIDSTRY_CD,
LIC_IN_MULTI_ST_IND,
PRTY_DESC,
INTRNL_ORG_END_DTTM,
LIFCYCL_CD,
PRTY_TYPE_CD,
PRCS_ID,
SRC_SYS_CD,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM,
TRANS_END_DTTM
)
SELECT
exp_intrnl_org_insert.in_INTRNL_ORG_PRTY_ID1 as INTRNL_ORG_PRTY_ID,
exp_intrnl_org_insert.in_SRC_STRT_DT1 as INTRNL_ORG_STRT_DTTM,
exp_intrnl_org_insert.in_Type1 as INTRNL_ORG_TYPE_CD,
exp_intrnl_org_insert.in_Subtype1 as INTRNL_ORG_SBTYPE_CD,
exp_intrnl_org_insert.in_TYPECODE1 as INTRNL_ORG_NUM,
exp_intrnl_org_insert.in_ORG_TYPE_CODE1 as ORG_TYPE_CD,
exp_intrnl_org_insert.in_GICS_SBIDSTRY_CD1 as GICS_SBIDSTRY_CD,
exp_intrnl_org_insert.in_LIC_IN_MULTI_ST_IND1 as LIC_IN_MULTI_ST_IND,
exp_intrnl_org_insert.in_PRTY_DESC1 as PRTY_DESC,
exp_intrnl_org_insert.in_SRC_END_DT1 as INTRNL_ORG_END_DTTM,
exp_intrnl_org_insert.in_LIFCYCL_CD1 as LIFCYCL_CD,
exp_intrnl_org_insert.in_PRTY_TYPE_CD1 as PRTY_TYPE_CD,
:PRCS_ID as PRCS_ID,
--exp_intrnl_org_insert.PRCS_ID1 as PRCS_ID,
exp_intrnl_org_insert.in_SYS_SRC_CD1 as SRC_SYS_CD,
exp_intrnl_org_insert.in_EDW_STRT_DTTM1 as EDW_STRT_DTTM,
exp_intrnl_org_insert.out_EDW_END_DTTM11 as EDW_END_DTTM,
exp_intrnl_org_insert.Trans_strt_dttm11 as TRANS_STRT_DTTM,
exp_intrnl_org_insert.TRANS_END_DTTM as TRANS_END_DTTM
FROM
exp_intrnl_org_insert;


-- Component tgt_intrnl_org_upd_ins, Type TARGET 
INSERT INTO DB_T_PROD_CORE.INTRNL_ORG
(
INTRNL_ORG_PRTY_ID,
INTRNL_ORG_STRT_DTTM,
INTRNL_ORG_TYPE_CD,
INTRNL_ORG_SBTYPE_CD,
INTRNL_ORG_NUM,
ORG_TYPE_CD,
GICS_SBIDSTRY_CD,
LIC_IN_MULTI_ST_IND,
PRTY_DESC,
INTRNL_ORG_END_DTTM,
LIFCYCL_CD,
PRTY_TYPE_CD,
PRCS_ID,
SRC_SYS_CD,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM
)
SELECT
exp_intrnl_org_upd_ins.lkp_INTRNL_ORG_PRTY_ID3 as INTRNL_ORG_PRTY_ID,
exp_intrnl_org_upd_ins.in_SRC_STRT_DT1 as INTRNL_ORG_STRT_DTTM,
exp_intrnl_org_upd_ins.lkp_INTRNL_ORG_TYPE_CD3 as INTRNL_ORG_TYPE_CD,
exp_intrnl_org_upd_ins.lkp_INTRNL_ORG_SBTYPE_CD3 as INTRNL_ORG_SBTYPE_CD,
exp_intrnl_org_upd_ins.lkp_INTRNL_ORG_NUM3 as INTRNL_ORG_NUM,
exp_intrnl_org_upd_ins.in_ORG_TYPE_CODE3 as ORG_TYPE_CD,
exp_intrnl_org_upd_ins.in_GICS_SBIDSTRY_CD3 as GICS_SBIDSTRY_CD,
exp_intrnl_org_upd_ins.in_LIC_IN_MULTI_ST_IND3 as LIC_IN_MULTI_ST_IND,
exp_intrnl_org_upd_ins.in_PRTY_DESC3 as PRTY_DESC,
exp_intrnl_org_upd_ins.in_SRC_END_DT1 as INTRNL_ORG_END_DTTM,
exp_intrnl_org_upd_ins.in_LIFCYCL_CD3 as LIFCYCL_CD,
exp_intrnl_org_upd_ins.in_PRTY_TYPE_CD3 as PRTY_TYPE_CD,
exp_intrnl_org_upd_ins.PRCS_ID3 as PRCS_ID,
exp_intrnl_org_upd_ins.lkp_SYS_SRC_CD3 as SRC_SYS_CD,
exp_intrnl_org_upd_ins.in_EDW_STRT_DTTM3 as EDW_STRT_DTTM,
exp_intrnl_org_upd_ins.in_EDW_END_DTTM3 as EDW_END_DTTM,
exp_intrnl_org_upd_ins.out_TRANS_STRT_DTTM as TRANS_STRT_DTTM
FROM
exp_intrnl_org_upd_ins;


END; ';