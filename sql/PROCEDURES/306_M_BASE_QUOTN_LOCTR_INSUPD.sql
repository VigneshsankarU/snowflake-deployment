-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_QUOTN_LOCTR_INSUPD("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
DECLARE 

start_dttm TIMESTAMP;
end_dttm TIMESTAMP;
PRCS_ID INTEGER;
P_DEFAULT_STR_CD char;
var_ContactroleTypecode char;

v_STREET_ADDR_ID_RISK int;
v_QUOTN_LOCTR_ROLE_TYPE_CD int;
STREET_ADDR_ID_ARGM int;


BEGIN 
start_dttm := CURRENT_TIMESTAMP();
end_dttm := CURRENT_TIMESTAMP();
PRCS_ID := 1;  
v_STREET_ADDR_ID_RISK:=1;
v_QUOTN_LOCTR_ROLE_TYPE_CD :=1;
STREET_ADDR_ID_ARGM :=1;
-- Component LKP_TERADATA_ETL_REF_XLAT_QUOTN_LOCTR_ROLE_CD, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_QUOTN_LOCTR_ROLE_CD AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''QUOTN_LOCTR_ROLE_TYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS''

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component SQ_quotn_loctr_x, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_quotn_loctr_x AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as JobNumber,
$2 as BranchNumber,
$3 as State,
$4 as Country,
$5 as AGMT_LOCTR_ROLE_TYPE_CD,
$6 as Eff_DT,
$7 as End_DT,
$8 as UpdateTime,
$9 as Retired,
$10 as Rnk,
$11 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select distinct pc_job.jobnumber_stg jobnumber,

pc_policyperiod.BranchNumber_stg branchnumber,

pctl_jurisdiction.TYPECODE_stg state,

''US'' as country,

''QUOTN_LOCTR_ROLE_TYPE7'' as agmt_loctr_role_type_cd,

pc_policyperiod.EditeffectiveDate_stg as eff_dt,

pc_policyperiod.periodend_stg as end_dt,

pc_policyperiod.UpdateTime_stg as updatetime,

case when pc_policyperiod.retired_stg=0  and pc_job.retired_stg=0 then 0 else 1 end as retired

,row_number()  over(partition by pc_job.jobnumber_stg,pc_policyperiod.BranchNumber_stg,agmt_loctr_role_type_cd  

order by pc_policyperiod.EditeffectiveDate_stg,pc_policyperiod.periodend_stg,pc_policyperiod.UpdateTime_stg)  as rnk

from DB_T_PROD_STAG.pc_policyperiod 

join DB_T_PROD_STAG.pctl_jurisdiction on pc_policyperiod.BaseState_stg=pctl_jurisdiction.id_stg

inner join DB_T_PROD_STAG.pc_job on pc_job.id_stg=pc_policyperiod.JobID_stg

inner join DB_T_PROD_STAG.pctl_job on pctl_job.id_stg=pc_job.Subtype_stg

inner join DB_T_PROD_STAG.pctl_policyperiodstatus on pctl_policyperiodstatus.id_stg=pc_policyperiod.Status_stg

Where  pctl_job.TYPECODE_stg in (''Submission'',''PolicyChange'',''Renewal'')

and pctl_policyperiodstatus.TYPECODE_stg <> ''Temporary''

and pctl_jurisdiction.TYPECODE_stg is not null

and pc_policyperiod.UpdateTime_stg > (:start_dttm)

and pc_policyperiod.UpdateTime_stg <= (:end_dttm)
) SRC
)
);


-- Component LKP_INSRNC_QUOTN, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_INSRNC_QUOTN AS
(
SELECT
LKP.QUOTN_ID,
SQ_quotn_loctr_x.source_record_id,
ROW_NUMBER() OVER(PARTITION BY SQ_quotn_loctr_x.source_record_id ORDER BY LKP.QUOTN_ID asc) RNK
FROM
SQ_quotn_loctr_x
LEFT JOIN (
SELECT INSRNC_QUOTN.QUOTN_ID AS QUOTN_ID, INSRNC_QUOTN.NK_JOB_NBR AS NK_JOB_NBR, INSRNC_QUOTN.VERS_NBR AS VERS_NBR FROM DB_T_PROD_CORE.INSRNC_QUOTN
QUALIFY ROW_NUMBER() OVER(PARTITION BY  INSRNC_QUOTN.NK_JOB_NBR, INSRNC_QUOTN.VERS_NBR,  INSRNC_QUOTN.SRC_SYS_CD  ORDER BY INSRNC_QUOTN.EDW_END_DTTM DESC) = 1
) LKP ON LKP.NK_JOB_NBR = SQ_quotn_loctr_x.JobNumber AND LKP.VERS_NBR = SQ_quotn_loctr_x.BranchNumber
QUALIFY RNK = 1
);


-- Component exp_pass_from_source, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_from_source AS
(
SELECT
LKP_INSRNC_QUOTN.QUOTN_ID as lkp_QUOTN_ID,
NULL as ADDRESSLINE1,
NULL as ADDRESSLINE2,
NULL as CITY,
SQ_quotn_loctr_x.State as STATE,
SQ_quotn_loctr_x.Country as COUNTRY,
NULL as POSTALCODE,
SQ_quotn_loctr_x.AGMT_LOCTR_ROLE_TYPE_CD as QUOTN_LOCTR_ROLE_TYPE_CD,
SQ_quotn_loctr_x.Eff_DT as EF_DT,
SQ_quotn_loctr_x.End_DT as END_DT,
SQ_quotn_loctr_x.UpdateTime as UpdateTime,
SQ_quotn_loctr_x.Retired as Retired,
SQ_quotn_loctr_x.Rnk as Rnk,
SQ_quotn_loctr_x.source_record_id
FROM
SQ_quotn_loctr_x
INNER JOIN LKP_INSRNC_QUOTN ON SQ_quotn_loctr_x.source_record_id = LKP_INSRNC_QUOTN.source_record_id
);


-- Component LKP_CTRY, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_CTRY AS
(
SELECT
LKP.CTRY_ID,
exp_pass_from_source.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_from_source.source_record_id ORDER BY LKP.CTRY_ID asc,LKP.CAL_TYPE_CD asc,LKP.ISO_3166_CTRY_NUM asc,LKP.GEOGRCL_AREA_SHRT_NAME asc,LKP.GEOGRCL_AREA_NAME asc,LKP.GEOGRCL_AREA_DESC asc,LKP.CURY_CD asc,LKP.GEOGRCL_AREA_STRT_DTTM asc,LKP.GEOGRCL_AREA_END_DTTM asc,LKP.PRCS_ID asc) RNK
FROM
exp_pass_from_source
LEFT JOIN (
SELECT
CTRY_ID,
CAL_TYPE_CD,
ISO_3166_CTRY_NUM,
GEOGRCL_AREA_SHRT_NAME,
GEOGRCL_AREA_NAME,
GEOGRCL_AREA_DESC,
CURY_CD,
GEOGRCL_AREA_STRT_DTTM,
GEOGRCL_AREA_END_DTTM,
PRCS_ID
FROM DB_T_PROD_CORE.CTRY
) LKP ON LKP.GEOGRCL_AREA_SHRT_NAME = exp_pass_from_source.COUNTRY
QUALIFY RNK = 1
);


-- Component LKP_TERR, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERR AS
(
SELECT
LKP.TERR_ID,
LKP.CTRY_ID,
exp_pass_from_source.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_from_source.source_record_id ORDER BY LKP.TERR_ID asc,LKP.TERR_TYPE_CD asc,LKP.CTRY_ID asc,LKP.RGN_ID asc,LKP.GEOGRCL_AREA_SHRT_NAME asc,LKP.GEOGRCL_AREA_NAME asc,LKP.GEOGRCL_AREA_DESC asc,LKP.CURY_CD asc,LKP.GEOGRCL_AREA_STRT_DTTM asc,LKP.GEOGRCL_AREA_END_DTTM asc,LKP.PRCS_ID asc) RNK1
FROM
exp_pass_from_source
INNER JOIN LKP_CTRY ON exp_pass_from_source.source_record_id = LKP_CTRY.source_record_id
LEFT JOIN (
SELECT
TERR_ID,
TERR_TYPE_CD,
CTRY_ID,
RGN_ID,
GEOGRCL_AREA_SHRT_NAME,
GEOGRCL_AREA_NAME,
GEOGRCL_AREA_DESC,
CURY_CD,
GEOGRCL_AREA_STRT_DTTM,
GEOGRCL_AREA_END_DTTM,
PRCS_ID
FROM DB_T_PROD_CORE.TERR
) LKP ON LKP.CTRY_ID = LKP_CTRY.CTRY_ID AND LKP.GEOGRCL_AREA_SHRT_NAME = exp_pass_from_source.STATE
QUALIFY RNK1 = 1
);


-- Component exp_data_transformation, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_data_transformation AS
(
SELECT
exp_pass_from_source.lkp_QUOTN_ID as QUOTN_ID,
exp_pass_from_source.ADDRESSLINE1 as ADDRESSLINE1,
exp_pass_from_source.ADDRESSLINE2 as ADDRESSLINE2,
exp_pass_from_source.CITY as CITY,
exp_pass_from_source.STATE as STATE,
exp_pass_from_source.COUNTRY as COUNTRY,
exp_pass_from_source.POSTALCODE as POSTALCODE,
LKP_TERR.TERR_ID as TERR_ID,
LKP_CTRY.CTRY_ID as CTRY_ID,
LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_QUOTN_LOCTR_ROLE_CD */ as v_QUOTN_LOCTR_ROLE_TYPE_CD,
v_QUOTN_LOCTR_ROLE_TYPE_CD as o_QUOTN_LOCTR_ROLE_TYPE_CD,
exp_pass_from_source.END_DT as END_DT,

CASE WHEN exp_pass_from_source.COUNTRY IS NOT NULL THEN exp_pass_from_source.COUNTRY ELSE CASE WHEN exp_pass_from_source.CITY IS NOT NULL THEN exp_pass_from_source.CITY ELSE CASE WHEN exp_pass_from_source.COUNTRY IS NULL and exp_pass_from_source.CITY IS NULL THEN LKP_TERR.TERR_ID ELSE LKP_TERR.TERR_ID  END END END as v_STREET_ADDR_ID_RISK,

/*
CASE WHEN STREET_ADDR_ID_TERR_ID_CNTY IS NOT NULL THEN STREET_ADDR_ID_TERR_ID_CNTY ELSE CASE WHEN STREET_ADDR_ID_TERR_ID_CITY IS NOT NULL THEN STREET_ADDR_ID_TERR_ID_CITY ELSE CASE WHEN STREET_ADDR_ID_TERR_ID_CNTY IS NULL and STREET_ADDR_ID_TERR_ID_CITY IS NULL THEN LKP_TERR.TERR_ID ELSE $3 END END END 
as v_STREET_ADDR_ID_RISK,*/
exp_pass_from_source.Retired as Retired,
CASE
  WHEN v_QUOTN_LOCTR_ROLE_TYPE_CD = ''RISK'' THEN v_STREET_ADDR_ID_RISK
  WHEN v_QUOTN_LOCTR_ROLE_TYPE_CD = ''AGRM'' THEN :STREET_ADDR_ID_ARGM
  WHEN v_QUOTN_LOCTR_ROLE_TYPE_CD = ''TAX'' THEN :STREET_ADDR_ID_ARGM
  WHEN v_QUOTN_LOCTR_ROLE_TYPE_CD = ''AGTWINST'' THEN LKP_TERR.TERR_ID
  WHEN v_QUOTN_LOCTR_ROLE_TYPE_CD = ''SOLD'' THEN LKP_TERR.TERR_ID
END as out_LOC_ID,
CURRENT_TIMESTAMP as out_EDW_STRT_DTTM,
TO_TIMESTAMP( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as out_EDW_END_DTTM,
:PRCS_ID as out_PRCS_ID,
DATE_TRUNC(DAY, exp_pass_from_source.EF_DT) as o_EFF_DT,
DATE_TRUNC(DAY, exp_pass_from_source.END_DT) as o_END_DT,
exp_pass_from_source.Rnk as Rnk,
exp_pass_from_source.UpdateTime as UpdateTime,
exp_pass_from_source.source_record_id,
row_number() over (partition by exp_pass_from_source.source_record_id order by exp_pass_from_source.source_record_id) as RNK1
FROM
exp_pass_from_source
INNER JOIN LKP_CTRY ON exp_pass_from_source.source_record_id = LKP_CTRY.source_record_id
INNER JOIN LKP_TERR ON LKP_CTRY.source_record_id = LKP_TERR.source_record_id
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_QUOTN_LOCTR_ROLE_CD LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = exp_pass_from_source.QUOTN_LOCTR_ROLE_TYPE_CD
QUALIFY RNK1 = 1
);


-- Component fil_null_loc_id, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE fil_null_loc_id AS
(
SELECT
exp_data_transformation.QUOTN_ID as QUOTN_ID,
exp_data_transformation.o_QUOTN_LOCTR_ROLE_TYPE_CD as o_QUOTN_LOCTR_ROLE_TYPE_CD,
exp_data_transformation.out_LOC_ID as in_LOC_ID,
exp_data_transformation.out_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_data_transformation.out_EDW_END_DTTM as in_EDW_END_DTTM,
exp_data_transformation.out_PRCS_ID as in_PRCS_ID,
exp_data_transformation.o_EFF_DT as in_EFF_DT,
exp_data_transformation.o_END_DT as in_END_DT,
exp_data_transformation.Retired as Retired,
exp_data_transformation.Rnk as Rnk,
exp_data_transformation.UpdateTime as UpdateTime,
exp_data_transformation.source_record_id
FROM
exp_data_transformation
WHERE exp_data_transformation.out_LOC_ID IS NOT NULL
);


-- Component exp_data_to_lkp, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_data_to_lkp AS
(
SELECT
fil_null_loc_id.QUOTN_ID as QUOTN_ID,
fil_null_loc_id.o_QUOTN_LOCTR_ROLE_TYPE_CD as o_QUOTN_LOCTR_ROLE_TYPE_CD,
fil_null_loc_id.in_EFF_DT as in_QUOTN_LOCTR_STRT_DT,
fil_null_loc_id.in_END_DT as in_QUOTN_LOCTR_END_DT,
fil_null_loc_id.in_LOC_ID as in_LOC_ID,
fil_null_loc_id.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
fil_null_loc_id.in_EDW_END_DTTM as in_EDW_END_DTTM,
fil_null_loc_id.in_PRCS_ID as in_PRCS_ID,
fil_null_loc_id.Retired as Retired,
TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as TRANS_END_DTTM,
fil_null_loc_id.Rnk as Rnk,
fil_null_loc_id.UpdateTime as UpdateTime,
fil_null_loc_id.source_record_id
FROM
fil_null_loc_id
);


-- Component LKP_QUOTN_LOCTR, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_QUOTN_LOCTR AS
(
SELECT
LKP.QUOTN_ID,
LKP.QUOTN_LOCTR_ROLE_TYPE_CD,
LKP.LOC_ID,
LKP.QUOTN_LOCTR_STRT_DTTM,
LKP.QUOTN_LOCTR_END_DTTM,
LKP.EDW_STRT_DTTM,
LKP.EDW_END_DTTM,
exp_data_to_lkp.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_data_to_lkp.source_record_id ORDER BY LKP.QUOTN_ID asc,LKP.QUOTN_LOCTR_ROLE_TYPE_CD asc,LKP.LOC_ID asc,LKP.QUOTN_LOCTR_STRT_DTTM asc,LKP.QUOTN_LOCTR_END_DTTM asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc) RNK
FROM
exp_data_to_lkp
LEFT JOIN (
SELECT QUOTN_LOCTR.QUOTN_LOCTR_STRT_DTTM as QUOTN_LOCTR_STRT_DTTM, QUOTN_LOCTR.QUOTN_LOCTR_END_DTTM as QUOTN_LOCTR_END_DTTM, QUOTN_LOCTR.EDW_STRT_DTTM as EDW_STRT_DTTM, QUOTN_LOCTR.EDW_END_DTTM as EDW_END_DTTM, QUOTN_LOCTR.QUOTN_ID as QUOTN_ID, QUOTN_LOCTR.QUOTN_LOCTR_ROLE_TYPE_CD as QUOTN_LOCTR_ROLE_TYPE_CD, QUOTN_LOCTR.LOC_ID as LOC_ID FROM DB_T_PROD_CORE.QUOTN_LOCTR 
QUALIFY ROW_NUMBER() OVER(PARTITION BY QUOTN_ID,QUOTN_LOCTR_ROLE_TYPE_CD ORDER BY EDW_END_DTTM desc) = 1
/* Where cast(EDW_END_DTTM as date format ''DD-MM-YYYY'') = cast(''31-12-9999'' as date) */
) LKP ON LKP.QUOTN_ID = exp_data_to_lkp.QUOTN_ID AND LKP.QUOTN_LOCTR_ROLE_TYPE_CD = exp_data_to_lkp.o_QUOTN_LOCTR_ROLE_TYPE_CD
QUALIFY RNK = 1
);


-- Component exp_insert_update_flag, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_insert_update_flag AS
(
SELECT
LKP_QUOTN_LOCTR.QUOTN_ID as lkp_QUOTN_ID,
LKP_QUOTN_LOCTR.QUOTN_LOCTR_ROLE_TYPE_CD as lkp_QUOTN_LOCTR_ROLE_TYPE_CD,
LKP_QUOTN_LOCTR.LOC_ID as lkp_LOC_ID,
LKP_QUOTN_LOCTR.QUOTN_LOCTR_STRT_DTTM as lkp_QUOTN_LOCTR_STRT_DT,
LKP_QUOTN_LOCTR.QUOTN_LOCTR_END_DTTM as lkp_QUOTN_LOCTR_END_DT,
LKP_QUOTN_LOCTR.EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
LKP_QUOTN_LOCTR.EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_data_to_lkp.QUOTN_ID as in_QUOTN_ID,
exp_data_to_lkp.o_QUOTN_LOCTR_ROLE_TYPE_CD as in_QUOTN_LOCTR_ROLE_TYPE_CD,
exp_data_to_lkp.in_QUOTN_LOCTR_STRT_DT as in_QUOTN_LOCTR_STRT_DT,
exp_data_to_lkp.in_QUOTN_LOCTR_END_DT as in_QUOTN_LOCTR_END_DT,
MD5 ( to_char ( exp_data_to_lkp.in_QUOTN_LOCTR_STRT_DT ) || to_char ( exp_data_to_lkp.in_QUOTN_LOCTR_END_DT ) || to_char ( exp_data_to_lkp.in_LOC_ID ) ) as in_chksum,
MD5 ( to_char ( LKP_QUOTN_LOCTR.QUOTN_LOCTR_STRT_DTTM ) || to_char ( LKP_QUOTN_LOCTR.QUOTN_LOCTR_END_DTTM ) || to_char ( LKP_QUOTN_LOCTR.LOC_ID ) ) as lkp_chksum,
CASE WHEN LKP_QUOTN_LOCTR.QUOTN_ID IS NULL THEN ''I'' ELSE ( CASE WHEN in_chksum <> lkp_chksum THEN ''U'' ELSE ''R'' END ) END as Flag,
exp_data_to_lkp.in_LOC_ID as in_LOC_ID,
exp_data_to_lkp.in_PRCS_ID as in_PRCS_ID,
exp_data_to_lkp.in_EDW_END_DTTM as in_EDW_END_DTTM,
NULL as NewLookupRow,
exp_data_to_lkp.Retired as Retired,
exp_data_to_lkp.TRANS_END_DTTM as TRANS_END_DTTM,
exp_data_to_lkp.Rnk as Rnk,
exp_data_to_lkp.UpdateTime as UpdateTime,
exp_data_to_lkp.source_record_id
FROM
exp_data_to_lkp
INNER JOIN LKP_QUOTN_LOCTR ON exp_data_to_lkp.source_record_id = LKP_QUOTN_LOCTR.source_record_id
);


-- Component RTR_QUOTN_LOCTR_INSERT, Type ROUTER Output Group INSERT
CREATE OR REPLACE TEMPORARY TABLE RTR_QUOTN_LOCTR_INSERT AS
(SELECT
exp_insert_update_flag.lkp_QUOTN_ID as lkp_QUOTN_ID,
exp_insert_update_flag.lkp_QUOTN_LOCTR_ROLE_TYPE_CD as lkp_QUOTN_LOCTR_ROLE_TYPE_CD,
exp_insert_update_flag.lkp_LOC_ID as lkp_LOC_ID,
exp_insert_update_flag.lkp_QUOTN_LOCTR_STRT_DT as lkp_QUOTN_LOCTR_STRT_DT,
exp_insert_update_flag.lkp_QUOTN_LOCTR_END_DT as lkp_QUOTN_LOCTR_END_DT,
exp_insert_update_flag.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_insert_update_flag.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_insert_update_flag.in_QUOTN_ID as in_QUOTN_ID,
exp_insert_update_flag.in_QUOTN_LOCTR_ROLE_TYPE_CD as in_QUOTN_LOCTR_ROLE_TYPE_CD,
exp_insert_update_flag.in_QUOTN_LOCTR_STRT_DT as in_QUOTN_LOCTR_STRT_DT,
exp_insert_update_flag.in_QUOTN_LOCTR_END_DT as in_QUOTN_LOCTR_END_DT,
exp_insert_update_flag.in_LOC_ID as in_LOC_ID,
exp_insert_update_flag.in_PRCS_ID as in_PRCS_ID,
exp_insert_update_flag.NewLookupRow as NewLookupRow,
exp_insert_update_flag.Retired as Retired,
exp_insert_update_flag.TRANS_END_DTTM as TRANS_END_DTTM,
exp_insert_update_flag.Flag as Flag,
exp_insert_update_flag.Rnk as Rnk,
exp_insert_update_flag.UpdateTime as UpdateTime,
exp_insert_update_flag.source_record_id
FROM
exp_insert_update_flag
WHERE exp_insert_update_flag.Flag = ''I'' AND exp_insert_update_flag.in_QUOTN_ID IS NOT NULL AND exp_insert_update_flag.in_LOC_ID IS NOT NULL OR ( exp_insert_update_flag.Flag = ''U'' AND exp_insert_update_flag.in_QUOTN_LOCTR_STRT_DT > exp_insert_update_flag.lkp_QUOTN_LOCTR_STRT_DT ) or ( exp_insert_update_flag.Retired = 0 AND exp_insert_update_flag.lkp_EDW_END_DTTM != TO_TIMESTAMP( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ) 
-- > first insert 
-- > insert incase of Change 
-- > retired earlier and now restored)
);


-- Component RTR_QUOTN_LOCTR_RETIRED, Type ROUTER Output Group RETIRED
CREATE OR REPLACE TEMPORARY TABLE RTR_QUOTN_LOCTR_RETIRED AS
(SELECT
exp_insert_update_flag.lkp_QUOTN_ID as lkp_QUOTN_ID,
exp_insert_update_flag.lkp_QUOTN_LOCTR_ROLE_TYPE_CD as lkp_QUOTN_LOCTR_ROLE_TYPE_CD,
exp_insert_update_flag.lkp_LOC_ID as lkp_LOC_ID,
exp_insert_update_flag.lkp_QUOTN_LOCTR_STRT_DT as lkp_QUOTN_LOCTR_STRT_DT,
exp_insert_update_flag.lkp_QUOTN_LOCTR_END_DT as lkp_QUOTN_LOCTR_END_DT,
exp_insert_update_flag.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_insert_update_flag.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_insert_update_flag.in_QUOTN_ID as in_QUOTN_ID,
exp_insert_update_flag.in_QUOTN_LOCTR_ROLE_TYPE_CD as in_QUOTN_LOCTR_ROLE_TYPE_CD,
exp_insert_update_flag.in_QUOTN_LOCTR_STRT_DT as in_QUOTN_LOCTR_STRT_DT,
exp_insert_update_flag.in_QUOTN_LOCTR_END_DT as in_QUOTN_LOCTR_END_DT,
exp_insert_update_flag.in_LOC_ID as in_LOC_ID,
exp_insert_update_flag.in_PRCS_ID as in_PRCS_ID,
exp_insert_update_flag.NewLookupRow as NewLookupRow,
exp_insert_update_flag.Retired as Retired,
exp_insert_update_flag.TRANS_END_DTTM as TRANS_END_DTTM,
exp_insert_update_flag.Flag as Flag,
exp_insert_update_flag.Rnk as Rnk,
exp_insert_update_flag.UpdateTime as UpdateTime,
exp_insert_update_flag.source_record_id
FROM
exp_insert_update_flag
WHERE exp_insert_update_flag.Flag = ''R'' and exp_insert_update_flag.Retired != 0 and exp_insert_update_flag.lkp_EDW_END_DTTM = TO_TIMESTAMP ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) 
-- > not insert or update , no change in values 
-- > but data is retired 
-- > update these records with CURRENT_TIMESTAMP

);


-- Component upd_quotn_loctr_upd_retired, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_quotn_loctr_upd_retired AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
RTR_QUOTN_LOCTR_RETIRED.lkp_QUOTN_ID as lkp_QUOTN_ID3,
RTR_QUOTN_LOCTR_RETIRED.lkp_QUOTN_LOCTR_ROLE_TYPE_CD as lkp_QUOTN_LOCTR_ROLE_TYPE_CD3,
RTR_QUOTN_LOCTR_RETIRED.lkp_LOC_ID as lkp_LOC_ID3,
RTR_QUOTN_LOCTR_RETIRED.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM3,
NULL as in_PRCS_ID3,
RTR_QUOTN_LOCTR_RETIRED.in_QUOTN_LOCTR_STRT_DT as in_QUOTN_LOCTR_STRT_DT4,
RTR_QUOTN_LOCTR_RETIRED.UpdateTime as UpdateTime3,
1 as UPDATE_STRATEGY_ACTION,
RTR_QUOTN_LOCTR_RETIRED.source_record_id
FROM
RTR_QUOTN_LOCTR_RETIRED
);


-- Component upd_quotn_loctr_insert, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_quotn_loctr_insert AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
RTR_QUOTN_LOCTR_INSERT.in_QUOTN_ID as QUOTN_ID1,
RTR_QUOTN_LOCTR_INSERT.in_QUOTN_LOCTR_ROLE_TYPE_CD as QUOTN_LOCTR_ROLE_TYPE_CD1,
RTR_QUOTN_LOCTR_INSERT.in_QUOTN_LOCTR_STRT_DT as in_QUOTN_LOCTR_STRT_DT,
RTR_QUOTN_LOCTR_INSERT.in_LOC_ID as out_LOC_ID1,
RTR_QUOTN_LOCTR_INSERT.in_QUOTN_LOCTR_END_DT as in_QUOTN_LOCTR_END_DT,
RTR_QUOTN_LOCTR_INSERT.in_PRCS_ID as out_PRCS_ID1,
RTR_QUOTN_LOCTR_INSERT.Retired as Retired1,
RTR_QUOTN_LOCTR_INSERT.TRANS_END_DTTM as TRANS_END_DTTM1,
RTR_QUOTN_LOCTR_INSERT.Rnk as Rnk1,
RTR_QUOTN_LOCTR_INSERT.UpdateTime as UpdateTime1,
0 as UPDATE_STRATEGY_ACTION,
RTR_QUOTN_LOCTR_INSERT.source_record_id
FROM
RTR_QUOTN_LOCTR_INSERT
);


-- Component exp_pass_to_tgt_insert, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_tgt_insert AS
(
SELECT
upd_quotn_loctr_insert.QUOTN_ID1 as QUOTN_ID1,
upd_quotn_loctr_insert.QUOTN_LOCTR_ROLE_TYPE_CD1 as QUOTN_LOCTR_ROLE_TYPE_CD1,
upd_quotn_loctr_insert.in_QUOTN_LOCTR_STRT_DT as in_QUOTN_LOCTR_STRT_DT,
upd_quotn_loctr_insert.out_LOC_ID1 as out_LOC_ID1,
upd_quotn_loctr_insert.in_QUOTN_LOCTR_END_DT as in_QUOTN_LOCTR_END_DT,
upd_quotn_loctr_insert.out_PRCS_ID1 as out_PRCS_ID1,
CASE WHEN upd_quotn_loctr_insert.Retired1 = 0 THEN to_timestamp ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) ELSE CURRENT_TIMESTAMP END as out_EDW_END_DTTM,
upd_quotn_loctr_insert.UpdateTime1 as UpdateTime1,
CASE WHEN upd_quotn_loctr_insert.Retired1 != 0 THEN upd_quotn_loctr_insert.UpdateTime1 ELSE to_timestamp ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) END as TRANS_END_DTTM11,
DATEADD (
  SECOND,
  (2 * (upd_quotn_loctr_insert.Rnk1 - 1)),
  CURRENT_TIMESTAMP()
) as out_EDW_STRT_DTTM,
upd_quotn_loctr_insert.source_record_id
FROM
upd_quotn_loctr_insert
);


-- Component tgt_QUOTN_LOCTR_insert, Type TARGET 
INSERT INTO DB_T_PROD_CORE.QUOTN_LOCTR
(
QUOTN_ID,
QUOTN_LOCTR_ROLE_TYPE_CD,
LOC_ID,
QUOTN_LOCTR_STRT_DTTM,
QUOTN_LOCTR_END_DTTM,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM,
TRANS_END_DTTM
)
SELECT
exp_pass_to_tgt_insert.QUOTN_ID1 as QUOTN_ID,
exp_pass_to_tgt_insert.QUOTN_LOCTR_ROLE_TYPE_CD1 as QUOTN_LOCTR_ROLE_TYPE_CD,
exp_pass_to_tgt_insert.out_LOC_ID1 as LOC_ID,
exp_pass_to_tgt_insert.in_QUOTN_LOCTR_STRT_DT as QUOTN_LOCTR_STRT_DTTM,
exp_pass_to_tgt_insert.in_QUOTN_LOCTR_END_DT as QUOTN_LOCTR_END_DTTM,
exp_pass_to_tgt_insert.out_PRCS_ID1 as PRCS_ID,
exp_pass_to_tgt_insert.out_EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_pass_to_tgt_insert.out_EDW_END_DTTM as EDW_END_DTTM,
exp_pass_to_tgt_insert.UpdateTime1 as TRANS_STRT_DTTM,
exp_pass_to_tgt_insert.TRANS_END_DTTM11 as TRANS_END_DTTM
FROM
exp_pass_to_tgt_insert;


-- Component tgt_QUOTN_LOCTR_insert, Type Post SQL 
UPDATE  DB_T_PROD_CORE.QUOTN_LOCTR FROM

(SELECT	distinct QUOTN_ID,QUOTN_LOCTR_ROLE_TYPE_CD,LOC_ID,EDW_STRT_DTTM,

max(TRANS_STRT_DTTM) over (partition by QUOTN_ID,QUOTN_LOCTR_ROLE_TYPE_CD ORDER BY EDW_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND'' 

 as lead,max(EDW_STRT_DTTM) over (partition by QUOTN_ID,QUOTN_LOCTR_ROLE_TYPE_CD ORDER BY EDW_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND'' 

 as EDW_lead

FROM	DB_T_PROD_CORE.QUOTN_LOCTR 

 ) a

set TRANS_END_DTTM=  A.lead,

EDW_END_DTTM=A.EDW_lead

where  QUOTN_LOCTR.EDW_STRT_DTTM = A.EDW_STRT_DTTM

and QUOTN_LOCTR.QUOTN_ID=A.QUOTN_ID 

AND QUOTN_LOCTR.QUOTN_LOCTR_ROLE_TYPE_CD=A.QUOTN_LOCTR_ROLE_TYPE_CD

AND QUOTN_LOCTR.LOC_ID=A.LOC_ID

and QUOTN_LOCTR.TRANS_STRT_DTTM <>QUOTN_LOCTR.TRANS_END_DTTM

and lead is not null;


-- Component exp_pass_to_tgt_upd_retired, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_tgt_upd_retired AS
(
SELECT
upd_quotn_loctr_upd_retired.lkp_QUOTN_ID3 as QUOTN_ID3,
upd_quotn_loctr_upd_retired.lkp_QUOTN_LOCTR_ROLE_TYPE_CD3 as QUOTN_LOCTR_ROLE_TYPE_CD3,
upd_quotn_loctr_upd_retired.lkp_LOC_ID3 as lkp_LOC_ID3,
upd_quotn_loctr_upd_retired.lkp_EDW_STRT_DTTM3 as lkp_EDW_STRT_DTTM3,
CURRENT_TIMESTAMP as out_EDW_END_DTTM,
upd_quotn_loctr_upd_retired.UpdateTime3 as UpdateTime3,
upd_quotn_loctr_upd_retired.source_record_id
FROM
upd_quotn_loctr_upd_retired
);


-- Component tgt_QUOTN_LOCTR_upd_retired, Type TARGET 
MERGE INTO DB_T_PROD_CORE.QUOTN_LOCTR
USING exp_pass_to_tgt_upd_retired ON (QUOTN_LOCTR.QUOTN_ID = exp_pass_to_tgt_upd_retired.QUOTN_ID3 AND QUOTN_LOCTR.QUOTN_LOCTR_ROLE_TYPE_CD = exp_pass_to_tgt_upd_retired.QUOTN_LOCTR_ROLE_TYPE_CD3 AND QUOTN_LOCTR.EDW_STRT_DTTM = exp_pass_to_tgt_upd_retired.lkp_EDW_STRT_DTTM3)
WHEN MATCHED THEN UPDATE
SET
QUOTN_ID = exp_pass_to_tgt_upd_retired.QUOTN_ID3,
QUOTN_LOCTR_ROLE_TYPE_CD = exp_pass_to_tgt_upd_retired.QUOTN_LOCTR_ROLE_TYPE_CD3,
LOC_ID = exp_pass_to_tgt_upd_retired.lkp_LOC_ID3,
EDW_STRT_DTTM = exp_pass_to_tgt_upd_retired.lkp_EDW_STRT_DTTM3,
EDW_END_DTTM = exp_pass_to_tgt_upd_retired.out_EDW_END_DTTM,
TRANS_END_DTTM = exp_pass_to_tgt_upd_retired.UpdateTime3;


END; ';