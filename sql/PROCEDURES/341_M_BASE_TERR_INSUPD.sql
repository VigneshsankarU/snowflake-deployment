-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_TERR_INSUPD("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '  
DECLARE start_dttm TIMESTAMP;
end_dttm TIMESTAMP;
PRCS_ID INTEGER;
NEXTVAL INTEGER;
V_TERR_ID INTEGER;
V_UPD_OR_INS INTEGER;
BEGIN 
start_dttm := CURRENT_TIMESTAMP();
end_dttm := CURRENT_TIMESTAMP();
PRCS_ID := 1;  
NEXTVAL := 1; 
V_TERR_ID:=1;
V_UPD_OR_INS:=1;

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


-- Component sq_bc_address, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_bc_address AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as state_TYPECODE,
$2 as NAME,
$3 as DESCRIPTION,
$4 as ctry_TYPECODE,
$5 as GEOGRCL_AREA_STRT_DT,
$6 as GEOGRCL_AREA_END_DT,
$7 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
Select state_TYPECODE, NAME, DESCRIPTION,case when (ctry_TYPECODE is null) then ''US'' else ctry_TYPECODE end ctry_TYPE_cd, STRT_DT,END_DT from

 ( SELECT	 bctl_state.TYPECODE_stg as state_TYPECODE, bctl_state.NAME_stg AS NAME,

		bctl_state.DESCRIPTION_stg AS DESCRIPTION, bctl_country.TYPECODE_stg as ctry_TYPECODE,

bc_address.createtime_stg as STRT_DT, cast(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6)) as END_DT

FROM	( SELECT	

		bc_address.CreateTime_stg, bc_address.Country_stg, 

	 bc_address.State_stg,bc_address.UpdateTime_stg

FROM DB_T_PROD_STAG.bc_address

WHERE	bc_address.UpdateTime_stg>(:start_dttm) 

	AND bc_address.UpdateTime_stg <= (:end_dttm)) bc_address 

left join DB_T_PROD_STAG.bctl_country 

	on bctl_country.ID_stg=bc_address.Country_stg  	

	join DB_T_PROD_STAG.bctl_state  

	on bc_address.State_stg=bctl_state.ID_stg 

UNION 

SELECT	 cctl_state.TYPECODE_stg as state_TYPECODE, cctl_state.NAME_stg AS NAME,

		cctl_state.DESCRIPTION_stg AS DESCRIPTION, cctl_country.TYPECODE_stg as ctry_TYPECODE,

cc_address.createtime_stg as STRT_DT, cast(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6)) as END_DT

FROM	( SELECT	

		cc_address.CreateTime_stg, cc_address.Country_stg, 

	 cc_address.State_stg,cc_address.UpdateTime_stg

FROM DB_T_PROD_STAG.cc_address

WHERE	cc_address.UpdateTime_stg>(:start_dttm) 

	AND cc_address.UpdateTime_stg <= (:end_dttm)) cc_address 

	join DB_T_PROD_STAG.cctl_state  

	on cc_address.State_stg=cctl_state.ID_stg 

left join DB_T_PROD_STAG.cctl_country 

	on cctl_country.ID_stg=cc_address.Country_stg 

UNION

SELECT	 pctl_state.TYPECODE_stg as state_TYPECODE, pctl_state.NAME_stg AS NAME,

		pctl_state.DESCRIPTION_stg AS DESCRIPTION, pctl_country.TYPECODE_stg as ctry_TYPECODE,

pc_address.createtime_stg as STRT_DT, cast(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6)) as END_DT

FROM	( SELECT	

		pc_address.CreateTime_stg, pc_address.Country_stg, 

	 pc_address.State_stg,pc_address.UpdateTime_stg

FROM DB_T_PROD_STAG.pc_address

WHERE	pc_address.UpdateTime_stg>(:start_dttm) 

	AND pc_address.UpdateTime_stg <= (:end_dttm)) pc_address 

left join DB_T_PROD_STAG.pctl_country 

	on pctl_country.ID_stg=pc_address.Country_stg  	

	join  DB_T_PROD_STAG.pctl_state  

	on pc_address.State_stg=pctl_state.ID_stg

UNION

SELECT	 pctl_state.TYPECODE_stg as state_TYPECODE, pctl_state.NAME_stg AS NAME,

		pctl_state.DESCRIPTION_stg AS DESCRIPTION, pctl_country.TYPECODE_stg as ctry_TYPECODE,

pc_policylocation.createtime_stg as STRT_DT, cast(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6)) as END_DT

FROM	  ( SELECT		pc_policylocation.CreateTime_stg, pc_policylocation.StateInternal_stg,

		pc_policylocation.CountryInternal_stg,pc_policylocation.UpdateTime_stg

FROM DB_T_PROD_STAG.pc_policylocation

where	

pc_policylocation.UpdateTime_stg > (:start_dttm)

	and pc_policylocation.UpdateTime_stg <= (:end_dttm)) pc_policylocation 

left join DB_T_PROD_STAG.pctl_country 

	on	 pctl_country.ID_stg=pc_policylocation.CountryInternal_stg  

	join DB_T_PROD_STAG.pctl_state 

	on	pc_policylocation.StateInternal_stg=pctl_state.ID_stg

UNION

select	pc_loc_master_x.state_typecode as state_TYPECODE,pc_loc_master_x.state_Name AS NAME, 

pc_loc_master_x.state_DESC AS DESCRIPTION, pc_loc_master_x.country_typecode as ctry_typecode,

cast(''01/01/1900'' as date) as STRT_DT, cast(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6)) as END_DT

from	( select	  state1.country_typecode, state1.state_typecode, state1.state_DESC, state1.state_name

from	

(	select	pc_zone.id_stg,pc_zone.name_stg as state_typecode, pctl_state.name_stg as state_name,

	pctl_state.DESCRIPTION_stg AS state_DESC ,pctl_country.TYPECODE_stg as country_typecode,

			pctl_country.NAME_stg as cntry_name, pctl_country.DESCRIPTION_stg as cntry_desc

	from	DB_T_PROD_STAG.pc_zone 

	inner join DB_T_PROD_STAG.pctl_zonetype 

		on pctl_zonetype.id_stg=pc_zone.ZoneType_stg

	inner join DB_T_PROD_STAG.pctl_country 

		on pctl_country.id_stg=pc_zone.Country_stg

	inner join DB_T_PROD_STAG.pctl_state 

		on pctl_state.TYPECODE_stg=pc_zone.name_stg

	where	pctl_zonetype.TYPECODE_stg=''state'') state1,

(	select	pc_zone.id_stg,pc_zone.name_stg 

	from	DB_T_PROD_STAG.pc_zone 

	inner join DB_T_PROD_STAG.pctl_zonetype 

		on pctl_zonetype.id_stg=pc_zone.ZoneType_stg

	inner join DB_T_PROD_STAG.pctl_country 

		on pctl_country.id_stg=pc_zone.Country_stg

	where	pctl_zonetype.TYPECODE_stg=''county'') county,

(	select	pc_zone.id_stg,pc_zone.name_stg 

	from	DB_T_PROD_STAG.pc_zone 

	inner join DB_T_PROD_STAG.pctl_zonetype 

		on pctl_zonetype.id_stg=pc_zone.ZoneType_stg

	inner join DB_T_PROD_STAG.pctl_country 

		on pctl_country.id_stg=pc_zone.Country_stg

	where	pctl_zonetype.TYPECODE_stg=''city'') city,

(	select	pc_zone.id_stg,pc_zone.name_stg 

	from	DB_T_PROD_STAG.pc_zone 

	inner join DB_T_PROD_STAG.pctl_zonetype 

		on pctl_zonetype.id_stg=pc_zone.ZoneType_stg

	inner join DB_T_PROD_STAG.pctl_country 

		on pctl_country.id_stg=pc_zone.Country_stg

	where	pctl_zonetype.TYPECODE_stg=''zip'') zip,

DB_T_PROD_STAG.pc_zone_link zl_state_county,

DB_T_PROD_STAG.pc_zone_link zl_county_city,

DB_T_PROD_STAG.pc_zone_link zl_city_zip

where	

state1.id_stg=zl_state_county.Zone1id_stg 

	and zl_state_county.Zone2id_stg=county.id_stg

	and county.id_stg=zl_county_city.Zone1id_stg 

	and zl_county_city.Zone2id_stg=city.id_stg

	and city.id_stg=zl_city_zip.Zone1id_stg 

	and zl_city_zip.Zone2id_stg=zip.id_stg )pc_loc_master_x

union

SELECT	 distinct pctl_jurisdiction.TYPECODE_stg as state_TYPECODE,pctl_jurisdiction.NAME_stg AS NAME, 

		pctl_jurisdiction.DESCRIPTION_stg AS DESCRIPTION, ''US'' as ctry_TYPECODE,

		cast(''01/01/1900'' as date) as STRT_DT, cast(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6)) as END_DT

FROM	 (SELECT id_stg, basestate_stg,UpdateTime_stg FROM DB_T_PROD_STAG.pc_policyperiod

where pc_policyperiod.UpdateTime_stg > (:start_dttm)

	and pc_policyperiod.UpdateTime_stg <= (:end_dttm)) pc_policyperiod,DB_T_PROD_STAG.pctl_jurisdiction  

WHERE pctl_jurisdiction.id_stg=pc_policyperiod.basestate_stg 

) A  where state_TYPECODE is not null QUALIFY ROW_NUMBER() OVER(PARTITION BY  state_TYPECODE,DESCRIPTION,ctry_TYPE_cd ORDER BY STRT_DT DESC) = 1

) SRC
)
);


-- Component exp_pass_from_source_bc, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_from_source_bc AS
(
SELECT
sq_bc_address.state_TYPECODE as state_TYPECODE,
sq_bc_address.NAME as NAME,
sq_bc_address.DESCRIPTION as DESCRIPTION,
sq_bc_address.ctry_TYPECODE as ctry_TYPECODE,
sq_bc_address.GEOGRCL_AREA_STRT_DT as GEOGRCL_AREA_STRT_DT,
sq_bc_address.GEOGRCL_AREA_END_DT as GEOGRCL_AREA_END_DT,
sq_bc_address.source_record_id
FROM
sq_bc_address
);

-- Component exp_data_transform_bc, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_data_transform_bc AS
(
SELECT
exp_pass_from_source_bc.NAME as NAME,
exp_pass_from_source_bc.DESCRIPTION as DESCRIPTION,
exp_pass_from_source_bc.state_TYPECODE as state_TYPECODE,
exp_pass_from_source_bc.GEOGRCL_AREA_STRT_DT as GEOGRCL_AREA_STRT_DT,
exp_pass_from_source_bc.GEOGRCL_AREA_END_DT as GEOGRCL_AREA_END_DT,
LKP_1.CTRY_ID /* replaced lookup LKP_CTRY */ as v_ctry_id,
Vterr.terr_id v_terr_id,
''LOCTR_SBTYPE3'' as v_loctr_sbtype_val,
''GEOGRCL_AREA_SBTYPE2'' as v_geogrcl_area_sbtype_val,
LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_LOCTR_SBTYPE */ as v_loctr_sbtype,
LKP_3.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_GEOGRCL_AREA_SBTYPE */ as v_geogrcl_area_sbtype,
:v_upd_or_ins v_upd_or_ins,
LKP_1.CTRY_ID as out_ctry_ID,
NULL as out_terr_id,
v_loctr_sbtype as out_loctr_sbtype,
v_geogrcl_area_sbtype as out_geogrcl_area_sbtype,
:PRCS_ID as out_process_id,
exp_pass_from_source_bc.source_record_id,
row_number() over (partition by exp_pass_from_source_bc.source_record_id order by exp_pass_from_source_bc.source_record_id) as RNK
FROM
exp_pass_from_source_bc
LEFT JOIN LKP_CTRY LKP_1 ON LKP_1.GEOGRCL_AREA_SHRT_NAME = exp_pass_from_source_bc.ctry_TYPECODE
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_LOCTR_SBTYPE LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = v_loctr_sbtype_val
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_GEOGRCL_AREA_SBTYPE LKP_3 ON LKP_3.SRC_IDNTFTN_VAL = v_geogrcl_area_sbtype_val
left join DB_T_PROD_CORE.TERR  Vterr ON Vterr.TERR_TYPE_CD = LKP_1.GEOGRCL_AREA_SHRT_NAME and Vterr.CTRY_ID = LKP_1.CTRY_ID 
QUALIFY RNK = 1
);


-- Component LKP_TERR_CDC, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERR_CDC AS
(
SELECT
LKP.TERR_ID,
LKP.GEOGRCL_AREA_NAME,
LKP.GEOGRCL_AREA_DESC,
LKP.GEOGRCL_AREA_STRT_DTTM,
LKP.GEOGRCL_AREA_END_DTTM,
LKP.LOCTR_SBTYPE_CD,
LKP.GEOGRCL_AREA_SBTYPE_CD,
LKP.EDW_STRT_DTTM,
LKP.EDW_END_DTTM,
exp_data_transform_bc.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_data_transform_bc.source_record_id ORDER BY LKP.TERR_ID asc,LKP.CTRY_ID asc,LKP.GEOGRCL_AREA_SHRT_NAME asc,LKP.GEOGRCL_AREA_NAME asc,LKP.GEOGRCL_AREA_DESC asc,LKP.GEOGRCL_AREA_STRT_DTTM asc,LKP.GEOGRCL_AREA_END_DTTM asc,LKP.LOCTR_SBTYPE_CD asc,LKP.GEOGRCL_AREA_SBTYPE_CD asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc) RNK
FROM
exp_data_transform_bc
LEFT JOIN (
SELECT TERR.TERR_ID as TERR_ID, TERR.GEOGRCL_AREA_NAME as GEOGRCL_AREA_NAME, TERR.GEOGRCL_AREA_DESC as GEOGRCL_AREA_DESC, TERR.GEOGRCL_AREA_STRT_DTTM as GEOGRCL_AREA_STRT_DTTM, TERR.GEOGRCL_AREA_END_DTTM as GEOGRCL_AREA_END_DTTM, TERR.LOCTR_SBTYPE_CD as LOCTR_SBTYPE_CD, TERR.GEOGRCL_AREA_SBTYPE_CD as GEOGRCL_AREA_SBTYPE_CD, TERR.EDW_STRT_DTTM as EDW_STRT_DTTM, TERR.EDW_END_DTTM as EDW_END_DTTM, TERR.CTRY_ID as CTRY_ID, TERR.GEOGRCL_AREA_SHRT_NAME as GEOGRCL_AREA_SHRT_NAME 
FROM DB_T_PROD_CORE.TERR 
WHERE CAST(TERR.EDW_END_DTTM AS DATE)=to_date(''9999-12-31'' )
) LKP ON LKP.CTRY_ID = exp_data_transform_bc.out_ctry_ID AND LKP.GEOGRCL_AREA_SHRT_NAME = exp_data_transform_bc.state_TYPECODE
QUALIFY RNK = 1
);


-- Component exp_ins_upd, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_ins_upd AS
(
SELECT
LKP_TERR_CDC.TERR_ID as lkp_TERR_ID,
LKP_TERR_CDC.GEOGRCL_AREA_NAME as lkp_GEOGRCL_AREA_NAME,
LKP_TERR_CDC.GEOGRCL_AREA_DESC as lkp_GEOGRCL_AREA_DESC,
LKP_TERR_CDC.GEOGRCL_AREA_STRT_DTTM as lkp_GEOGRCL_AREA_STRT_DT,
LKP_TERR_CDC.GEOGRCL_AREA_END_DTTM as lkp_GEOGRCL_AREA_END_DT,
LKP_TERR_CDC.LOCTR_SBTYPE_CD as lkp_LOCTR_SBTYPE_CD,
LKP_TERR_CDC.GEOGRCL_AREA_SBTYPE_CD as lkp_GEOGRCL_AREA_SBTYPE_CD,
LKP_TERR_CDC.EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
LKP_TERR_CDC.EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_data_transform_bc.GEOGRCL_AREA_STRT_DT as GEOGRCL_AREA_STRT_DT,
exp_data_transform_bc.GEOGRCL_AREA_END_DT as GEOGRCL_AREA_END_DT,
exp_data_transform_bc.out_ctry_ID as out_ctry_ID,
exp_data_transform_bc.out_terr_id as out_terr_id,
exp_data_transform_bc.out_loctr_sbtype as out_loctr_sbtype,
exp_data_transform_bc.out_geogrcl_area_sbtype as out_geogrcl_area_sbtype,
exp_data_transform_bc.out_process_id as out_process_id,
exp_data_transform_bc.state_TYPECODE as state_TYPECODE,
exp_data_transform_bc.NAME as NAME,
exp_data_transform_bc.DESCRIPTION as DESCRIPTION,
MD5 ( ltrim ( rtrim ( upper ( LKP_TERR_CDC.GEOGRCL_AREA_NAME ) ) ) || ltrim ( rtrim ( upper ( LKP_TERR_CDC.GEOGRCL_AREA_DESC ) ) ) || ltrim ( rtrim ( LKP_TERR_CDC.LOCTR_SBTYPE_CD ) ) || ltrim ( rtrim ( LKP_TERR_CDC.GEOGRCL_AREA_SBTYPE_CD ) ) ) as v_lkp_chksm,
MD5 ( ltrim ( rtrim ( upper ( exp_data_transform_bc.NAME ) ) ) || ltrim ( rtrim ( upper ( exp_data_transform_bc.DESCRIPTION ) ) ) || ltrim ( rtrim ( exp_data_transform_bc.out_loctr_sbtype ) ) || ltrim ( rtrim ( exp_data_transform_bc.out_geogrcl_area_sbtype ) ) ) as v_src_chksm,
CASE WHEN v_lkp_chksm IS NULL THEN ''I'' ELSE CASE WHEN v_lkp_chksm != v_src_chksm THEN ''U'' ELSE ''R'' END END as o_flag,
CURRENT_TIMESTAMP as out_end_strt_dttm,
exp_data_transform_bc.source_record_id
FROM
exp_data_transform_bc
INNER JOIN LKP_TERR_CDC ON exp_data_transform_bc.source_record_id = LKP_TERR_CDC.source_record_id
);


-- Component rtr_update_insert_bc_INS, Type ROUTER Output Group INS
CREATE OR REPLACE TEMPORARY TABLE rtr_update_insert_bc_INS AS
(SELECT
exp_ins_upd.state_TYPECODE as Typecode,
exp_ins_upd.NAME as Name,
exp_ins_upd.DESCRIPTION as Description,
exp_ins_upd.o_flag as UPD_OR_INS,
exp_ins_upd.out_ctry_ID as out_ctry_ID,
exp_ins_upd.out_process_id as out_process_id,
exp_ins_upd.out_loctr_sbtype as out_loctr_sbtype,
exp_ins_upd.out_geogrcl_area_sbtype as out_geogrcl_area_sbtype,
exp_ins_upd.GEOGRCL_AREA_STRT_DT as GEOGRCL_AREA_STRT_DT,
exp_ins_upd.GEOGRCL_AREA_END_DT as GEOGRCL_AREA_END_DT,
exp_ins_upd.lkp_TERR_ID as lkp_TERR_ID,
NULL as lkp_TERR_TYPE_CD,
NULL as lkp_RGN_ID,
exp_ins_upd.lkp_GEOGRCL_AREA_NAME as lkp_GEOGRCL_AREA_NAME,
exp_ins_upd.lkp_GEOGRCL_AREA_DESC as lkp_GEOGRCL_AREA_DESC,
NULL as lkp_CURY_CD,
exp_ins_upd.lkp_GEOGRCL_AREA_STRT_DT as lkp_GEOGRCL_AREA_STRT_DT,
exp_ins_upd.lkp_GEOGRCL_AREA_END_DT as lkp_GEOGRCL_AREA_END_DT,
exp_ins_upd.lkp_LOCTR_SBTYPE_CD as lkp_LOCTR_SBTYPE_CD,
exp_ins_upd.lkp_GEOGRCL_AREA_SBTYPE_CD as lkp_GEOGRCL_AREA_SBTYPE_CD,
exp_ins_upd.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_ins_upd.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_ins_upd.out_end_strt_dttm as out_end_strt_dttm,
exp_ins_upd.source_record_id
FROM
exp_ins_upd
WHERE exp_ins_upd.o_flag = ''I'' 
-- or exp_ins_upd.o_flag = ''U'' and exp_ins_upd.lkp_EDW_END_DTTM = TO_TIMESTAMP ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
);


-- Component upd_insert_bc, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_insert_bc AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_update_insert_bc_INS.Typecode as Typecode,
rtr_update_insert_bc_INS.Name as Name,
rtr_update_insert_bc_INS.Description as Description,
rtr_update_insert_bc_INS.out_ctry_ID as out_ctry_ID1,
rtr_update_insert_bc_INS.out_process_id as out_process_id1,
rtr_update_insert_bc_INS.out_loctr_sbtype as out_loctr_sbtype1,
rtr_update_insert_bc_INS.out_geogrcl_area_sbtype as out_geogrcl_area_sbtype1,
rtr_update_insert_bc_INS.GEOGRCL_AREA_STRT_DT as GEOGRCL_AREA_STRT_DT1,
rtr_update_insert_bc_INS.GEOGRCL_AREA_END_DT as GEOGRCL_AREA_END_DT1,
rtr_update_insert_bc_INS.out_end_strt_dttm as out_end_strt_dttm1,
0 as UPDATE_STRATEGY_ACTION,
rtr_update_insert_bc_INS.source_record_id
FROM
rtr_update_insert_bc_INS
);


-- Component exp_pass_to_target_insert_bc, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_target_insert_bc AS
(
SELECT
upd_insert_bc.Typecode as Typecode,
upd_insert_bc.Name as Name,
upd_insert_bc.Description as Description,
upd_insert_bc.out_ctry_ID1 as out_ctry_ID1,
upd_insert_bc.out_process_id1 as out_process_id1,
upd_insert_bc.out_loctr_sbtype1 as out_loctr_sbtype1,
upd_insert_bc.out_geogrcl_area_sbtype1 as out_geogrcl_area_sbtype1,
SEQ_LOC.NEXTVAL as out_TERR_ID,
TO_timestamp ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as EDW_END_DTTM,
upd_insert_bc.GEOGRCL_AREA_STRT_DT1 as GEOGRCL_AREA_STRT_DT1,
upd_insert_bc.GEOGRCL_AREA_END_DT1 as GEOGRCL_AREA_END_DT1,
upd_insert_bc.out_end_strt_dttm1 as out_end_strt_dttm1,
upd_insert_bc.source_record_id
FROM
upd_insert_bc
);


-- Component tgt_terr_insert_bc, Type TARGET 
INSERT INTO DB_T_PROD_CORE.TERR
(
TERR_ID,
CTRY_ID,
GEOGRCL_AREA_SHRT_NAME,
GEOGRCL_AREA_NAME,
GEOGRCL_AREA_DESC,
GEOGRCL_AREA_STRT_DTTM,
GEOGRCL_AREA_END_DTTM,
LOCTR_SBTYPE_CD,
GEOGRCL_AREA_SBTYPE_CD,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM
)
SELECT
exp_pass_to_target_insert_bc.out_TERR_ID as TERR_ID,
exp_pass_to_target_insert_bc.out_ctry_ID1 as CTRY_ID,
exp_pass_to_target_insert_bc.Typecode as GEOGRCL_AREA_SHRT_NAME,
exp_pass_to_target_insert_bc.Name as GEOGRCL_AREA_NAME,
exp_pass_to_target_insert_bc.Description as GEOGRCL_AREA_DESC,
exp_pass_to_target_insert_bc.GEOGRCL_AREA_STRT_DT1 as GEOGRCL_AREA_STRT_DTTM,
exp_pass_to_target_insert_bc.GEOGRCL_AREA_END_DT1 as GEOGRCL_AREA_END_DTTM,
exp_pass_to_target_insert_bc.out_loctr_sbtype1 as LOCTR_SBTYPE_CD,
exp_pass_to_target_insert_bc.out_geogrcl_area_sbtype1 as GEOGRCL_AREA_SBTYPE_CD,
exp_pass_to_target_insert_bc.out_process_id1 as PRCS_ID,
exp_pass_to_target_insert_bc.out_end_strt_dttm1 as EDW_STRT_DTTM,
exp_pass_to_target_insert_bc.EDW_END_DTTM as EDW_END_DTTM
FROM
exp_pass_to_target_insert_bc;


END; ';