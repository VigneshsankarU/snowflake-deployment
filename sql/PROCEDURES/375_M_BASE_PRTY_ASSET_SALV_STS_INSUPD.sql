-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_PRTY_ASSET_SALV_STS_INSUPD("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
declare
start_dttm timestamp;
end_dttm timestamp;
prcs_id integer;

BEGIN 
start_dttm := CURRENT_TIMESTAMP();
end_dttm := CURRENT_TIMESTAMP();
prcs_id := 1;

-- Component LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''PRTY_ASSET_CLASFCN'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM in ( ''derived'' ,''pcx_holineschcovitemcov_alfa.ChoiceTerm1'', ''contentlineitemschedule.typecode'', ''pctl_bp7classificationproperty.typecode'')

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS in (''DS'', ''GW'') 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
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


-- Component SQ_cc_prty_asset_cost_rcvry_x, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_cc_prty_asset_cost_rcvry_x AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as PolicySystemId,
$2 as type_asset,
$3 as classification_code,
$4 as src_cd,
$5 as amt_cd,
$6 as UPDATETIME,
$7 as salvagestatus_id,
$8 as Claimnumber,
$9 as CreateTime,
$10 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT	DISTINCT cc_prty_asset_cost_rcvry_x.id_stg, cc_prty_asset_cost_rcvry_x.type_stg,

		cc_prty_asset_cost_rcvry_x.classification_code_stg, cc_prty_asset_cost_rcvry_x.src_cd_stg,

		cc_prty_asset_cost_rcvry_x.amt_cd_stg, cc_prty_asset_cost_rcvry_x.updatetime_stg,

		cc_prty_asset_cost_rcvry_x.salvagestatus_alfa_stg,cc_prty_asset_cost_rcvry_x.claimnumber_stg,

		cc_prty_asset_cost_rcvry_x.CreateTime_stg 


FROM

 (     



select 

distinct 

cast(case 

when cc_vehicle.PolicySystemId_stg is null and cc_vehicle.Vin_stg is not null then   ''VIN:''||cc_vehicle.vin_stg 
when (cc_vehicle.PolicySystemId_stg  is null and cc_vehicle.Vin_stg  is null and cc_vehicle.LicensePlate_stg  is not null) then  ''LP:''||cc_vehicle.licenseplate_stg  

when (cc_vehicle.PolicySystemId_stg  is null and cc_vehicle.Vin_stg  is null and cc_vehicle.LicensePlate_stg  is null) then cc_vehicle.PublicID_stg  

else substr(PolicySystemId_stg,position('':'' in PolicySystemId_stg)+1,length(PolicySystemId_stg)-position('':''in PolicySystemId_stg)) 

end  as varchar(100)) as id_stg  

,''PRTY_ASSET_SBTYPE4''  as type_stg  ,''PRTY_ASSET_CLASFCN3'' as classification_code_stg ,

case when PolicySystemId_stg  is null then ''SRC_SYS6'' else ''SRC_SYS4'' end as src_cd_stg ,

cctl_salvagestatus.TYPECODE_stg  amt_cd_stg ,

cc_incident.updatetime_stg recov_dt_stg,

cc_incident.updatetime_stg ,

''PRTY_ASSET_SALV_STS'' Idntn_Code_stg,

salvagestatus_alfa_stg ,

cc_claim.claimnumber_stg ,cc_incident.CreateTime_stg 
from DB_T_PROD_STAG.cc_incident 
inner join (select cc_claim.* from DB_T_PROD_STAG.cc_claim 
inner join DB_T_PROD_STAG.cctl_claimstate on cc_claim.State_stg = cctl_claimstate.id_stg where cctl_claimstate.name_stg  <> ''Draft'') cc_claim on cc_incident.claimid_stg  = cc_claim.id_stg 

left outer join DB_T_PROD_STAG.cc_vehicle cc_vehicle on cc_incident.VehicleID_stg  = cc_vehicle.ID_stg 

inner join DB_T_PROD_STAG.cc_transaction on cc_transaction.ClaimID_stg =cc_claim.ID_stg  

inner join DB_T_PROD_STAG.cctl_transaction on cc_transaction.Subtype_stg =cctl_transaction.ID_stg  

inner join DB_T_PROD_STAG.cctl_incident on cc_incident.Subtype_stg  = cctl_incident.id_stg 

left outer join DB_T_PROD_STAG.cctl_recoverycategory on cc_transaction.RecoveryCategory_stg  = cctl_recoverycategory.ID_stg 

left outer join DB_T_PROD_STAG.cctl_salvagestatus on cctl_salvagestatus.id_stg =cc_incident.salvagestatus_alfa_stg 

/* where cctl_recoverycategory.TYPECODE=''salvage'' and cctl_transaction.TYPECODE=''Recovery''  */
where cc_incident.updatetime_stg  > (:start_dttm)

and cc_incident.updatetime_stg  <= (:end_dttm)
qualify	row_number () over (
partition by  
--id_stg,  -- causing amibiguity in the query
cast(case 

when cc_vehicle.PolicySystemId_stg is null and cc_vehicle.Vin_stg is not null then   ''VIN:''||cc_vehicle.vin_stg 
when (cc_vehicle.PolicySystemId_stg  is null and cc_vehicle.Vin_stg  is null and cc_vehicle.LicensePlate_stg  is not null) then  ''LP:''||cc_vehicle.licenseplate_stg  

when (cc_vehicle.PolicySystemId_stg  is null and cc_vehicle.Vin_stg  is null and cc_vehicle.LicensePlate_stg  is null) then cc_vehicle.PublicID_stg  

else substr(PolicySystemId_stg,position('':'' in PolicySystemId_stg)+1,length(PolicySystemId_stg)-position('':''in PolicySystemId_stg)) 

end  as varchar(100)),
type_stg,classification_code_stg,claimnumber_stg
order by  cc_incident.updatetime_stg desc,
		cc_incident.CreateTime_stg desc,recov_dt_stg desc  )=1


union



select 

distinct 

case 

when (cc_vehicle.PolicySystemId_stg  is null and cc_vehicle.Vin_stg  is not null) then   ''VIN:''||cc_vehicle.vin_stg 

when (cc_vehicle.PolicySystemId_stg  is null and cc_vehicle.Vin_stg  is null and cc_vehicle.LicensePlate_stg  is not null) then  ''LP:''||cc_vehicle.licenseplate_stg  

when (cc_vehicle.PolicySystemId_stg  is null and cc_vehicle.Vin_stg  is null and cc_vehicle.LicensePlate_stg  is null) then cc_vehicle.PublicID_stg  

else substr(PolicySystemId_stg,position('':''in PolicySystemId_stg)+1,length(PolicySystemId_stg)-position('':''in PolicySystemId_stg)) 

end as id_stg  

,''PRTY_ASSET_SBTYPE4''  as type_stg  ,''PRTY_ASSET_CLASFCN3'' as classification_code_stg ,

case when PolicySystemId_stg  is null then ''SRC_SYS6'' else ''SRC_SYS4'' end as src_cd_stg ,

''ASSETRECOV'' amt_cd_stg ,

DateVehicleRecovered_stg recov_dt_stg,

cc_incident.updatetime_stg,

''PRTY_ASSET_SALV_STS'' Idntn_Code_stg,

salvagestatus_alfa_stg,

cc_claim.claimnumber_stg, cc_incident.CreateTime_stg 

from DB_T_PROD_STAG.cc_incident 
inner join (select cc_claim.* from DB_T_PROD_STAG.cc_claim inner join DB_T_PROD_STAG.cctl_claimstate on cc_claim.State_stg = cctl_claimstate.id_stg  where cctl_claimstate.name_stg  <> ''Draft'') cc_claim on cc_incident.claimid_stg  = cc_claim.id_stg 

inner join DB_T_PROD_STAG.cc_vehicle on cc_incident.VehicleID_stg  = cc_vehicle.ID_stg 

left outer join DB_T_PROD_STAG.cctl_incident on cc_incident.Subtype_stg  = cctl_incident.id_stg 

where cc_incident.DateVehicleRecovered_stg  is not null

and cc_incident.updatetime_stg  > (:start_dttm)

and cc_incident.updatetime_stg  <= (:end_dttm)

)
cc_prty_asset_cost_rcvry_x

WHERE cc_prty_asset_cost_rcvry_x.Idntn_Code_stg=''PRTY_ASSET_SALV_STS'' 

AND id_stg is not null 

AND Claimnumber_stg is not null 

AND salvagestatus_alfa_stg is not null 

and amt_cd_stg in ('':Amt_cd'')
) SRC
)
);



-- Component exp_all_sources, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_all_sources AS
(
SELECT
SQ_cc_prty_asset_cost_rcvry_x.UPDATETIME as UPDATETIME,
SQ_cc_prty_asset_cost_rcvry_x.amt_cd as STATUS,
SQ_cc_prty_asset_cost_rcvry_x.PolicySystemId as in_fixedid,
CASE WHEN LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE */ IS NULL THEN ''UNK'' ELSE LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE */ END as out_prty_asset_sbtype_cd,
CASE WHEN LKP_3.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN */ IS NULL THEN ''UNK'' ELSE LKP_4.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN */ END as out_class_cd,
CASE WHEN LKP_5.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SRC_CD */ IS NULL THEN ''UNK'' ELSE LKP_6.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SRC_CD */ END as out_src_cd,
SQ_cc_prty_asset_cost_rcvry_x.salvagestatus_id as salvagestatus_id,
SQ_cc_prty_asset_cost_rcvry_x.CreateTime as CreateTime,
SQ_cc_prty_asset_cost_rcvry_x.Claimnumber as Claimnumber,
SQ_cc_prty_asset_cost_rcvry_x.source_record_id,
row_number() over (partition by SQ_cc_prty_asset_cost_rcvry_x.source_record_id order by SQ_cc_prty_asset_cost_rcvry_x.source_record_id) as RNK
FROM
SQ_cc_prty_asset_cost_rcvry_x
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = SQ_cc_prty_asset_cost_rcvry_x.type_asset
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = SQ_cc_prty_asset_cost_rcvry_x.type_asset
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN LKP_3 ON LKP_3.SRC_IDNTFTN_VAL = SQ_cc_prty_asset_cost_rcvry_x.classification_code
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN LKP_4 ON LKP_4.SRC_IDNTFTN_VAL = SQ_cc_prty_asset_cost_rcvry_x.classification_code
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_SRC_CD LKP_5 ON LKP_5.SRC_IDNTFTN_VAL = SQ_cc_prty_asset_cost_rcvry_x.src_cd
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_SRC_CD LKP_6 ON LKP_6.SRC_IDNTFTN_VAL = SQ_cc_prty_asset_cost_rcvry_x.src_cd
QUALIFY RNK = 1
);


-- Component LKP_CLM, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_CLM AS
(
SELECT
LKP.CLM_ID,
exp_all_sources.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_all_sources.source_record_id ORDER BY LKP.CLM_ID desc,LKP.CLM_TYPE_CD desc,LKP.CLM_MDIA_TYPE_CD desc,LKP.CLM_SUBMTL_TYPE_CD desc,LKP.ACDNT_TYPE_CD desc,LKP.CLM_CTGY_TYPE_CD desc,LKP.ADDL_INSRNC_PLN_IND desc,LKP.EMPLMT_RLTD_IND desc,LKP.ATTNY_INVLVMT_IND desc,LKP.CLM_NUM desc,LKP.CLM_PRIR_IND desc,LKP.PMT_MODE_CD desc,LKP.CLM_OBLGTN_TYPE_CD desc,LKP.SUBRGTN_ELGBL_CD desc,LKP.SUBRGTN_ELGBLY_RSN_CD desc,LKP.CURY_CD desc,LKP.INCDT_EV_ID desc,LKP.INSRD_AT_FAULT_IND desc,LKP.CVGE_IN_QUES_IND desc,LKP.EXTNT_OF_FIRE_DMG_TYPE_CD desc,LKP.VFYD_CLM_IND desc,LKP.PRCS_ID desc,LKP.CLM_STRT_DTTM desc,LKP.CLM_END_DTTM desc,LKP.EDW_STRT_DTTM desc,LKP.EDW_END_DTTM desc,LKP.SRC_SYS_CD desc,LKP.TRANS_STRT_DTTM desc,LKP.LGCY_CLM_NUM desc) RNK
FROM
exp_all_sources
LEFT JOIN (
SELECT CLM.CLM_ID as CLM_ID, CLM.CLM_TYPE_CD as CLM_TYPE_CD, CLM.CLM_MDIA_TYPE_CD as CLM_MDIA_TYPE_CD, CLM.CLM_SUBMTL_TYPE_CD as CLM_SUBMTL_TYPE_CD, CLM.ACDNT_TYPE_CD as ACDNT_TYPE_CD, CLM.CLM_CTGY_TYPE_CD as CLM_CTGY_TYPE_CD, CLM.ADDL_INSRNC_PLN_IND as ADDL_INSRNC_PLN_IND, CLM.EMPLMT_RLTD_IND as EMPLMT_RLTD_IND, CLM.ATTNY_INVLVMT_IND as ATTNY_INVLVMT_IND, CLM.CLM_PRIR_IND as CLM_PRIR_IND, CLM.PMT_MODE_CD as PMT_MODE_CD, CLM.CLM_OBLGTN_TYPE_CD as CLM_OBLGTN_TYPE_CD, CLM.SUBRGTN_ELGBL_CD as SUBRGTN_ELGBL_CD, CLM.SUBRGTN_ELGBLY_RSN_CD as SUBRGTN_ELGBLY_RSN_CD, CLM.CURY_CD as CURY_CD, CLM.INCDT_EV_ID as INCDT_EV_ID, CLM.INSRD_AT_FAULT_IND as INSRD_AT_FAULT_IND, CLM.CVGE_IN_QUES_IND as CVGE_IN_QUES_IND, CLM.EXTNT_OF_FIRE_DMG_TYPE_CD as EXTNT_OF_FIRE_DMG_TYPE_CD, CLM.VFYD_CLM_IND as VFYD_CLM_IND, CLM.PRCS_ID as PRCS_ID, CLM.CLM_STRT_DTTM as CLM_STRT_DTTM, CLM.CLM_END_DTTM as CLM_END_DTTM, CLM.EDW_STRT_DTTM as EDW_STRT_DTTM, CLM.EDW_END_DTTM as EDW_END_DTTM, CLM.TRANS_STRT_DTTM as TRANS_STRT_DTTM, CLM.LGCY_CLM_NUM as LGCY_CLM_NUM, CLM.CLM_NUM as CLM_NUM, CLM.SRC_SYS_CD as SRC_SYS_CD 
FROM db_t_prod_core.CLM  QUALIFY ROW_NUMBER() OVER(PARTITION BY CLM.CLM_NUM,CLM.SRC_SYS_CD  ORDER BY CLM.EDW_END_DTTM desc) = 1
) LKP ON LKP.CLM_NUM = exp_all_sources.Claimnumber AND LKP.SRC_SYS_CD = exp_all_sources.out_src_cd
QUALIFY RNK = 1
);


-- Component LKP_PRTY_ASSET_ID, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_PRTY_ASSET_ID AS
(
SELECT
LKP.PRTY_ASSET_ID,
exp_all_sources.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_all_sources.source_record_id ORDER BY LKP.PRTY_ASSET_ID asc,LKP.ASSET_HOST_ID_VAL asc,LKP.PRTY_ASSET_SBTYPE_CD asc,LKP.PRTY_ASSET_CLASFCN_CD asc,LKP.ASSET_INSRNC_HIST_TYPE_CD asc,LKP.ASSET_DESC asc,LKP.PRTY_ASSET_NAME asc,LKP.PRTY_ASSET_STRT_DTTM asc,LKP.PRTY_ASSET_END_DTTM asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc,LKP.SRC_SYS_CD asc) RNK
FROM
exp_all_sources
LEFT JOIN (
SELECT PRTY_ASSET.PRTY_ASSET_ID as PRTY_ASSET_ID, PRTY_ASSET.ASSET_INSRNC_HIST_TYPE_CD as ASSET_INSRNC_HIST_TYPE_CD, PRTY_ASSET.ASSET_DESC as ASSET_DESC, PRTY_ASSET.PRTY_ASSET_NAME as PRTY_ASSET_NAME, PRTY_ASSET.PRTY_ASSET_STRT_DTTM as PRTY_ASSET_STRT_DTTM, PRTY_ASSET.PRTY_ASSET_END_DTTM as PRTY_ASSET_END_DTTM, PRTY_ASSET.EDW_STRT_DTTM as EDW_STRT_DTTM, PRTY_ASSET.EDW_END_DTTM as EDW_END_DTTM, PRTY_ASSET.SRC_SYS_CD as SRC_SYS_CD, PRTY_ASSET.ASSET_HOST_ID_VAL as ASSET_HOST_ID_VAL, PRTY_ASSET.PRTY_ASSET_SBTYPE_CD as PRTY_ASSET_SBTYPE_CD, PRTY_ASSET.PRTY_ASSET_CLASFCN_CD as PRTY_ASSET_CLASFCN_CD 
FROM db_t_prod_core.PRTY_ASSET 
QUALIFY ROW_NUMBER() OVER(PARTITION BY  ASSET_HOST_ID_VAL,PRTY_ASSET_SBTYPE_CD,PRTY_ASSET_CLASFCN_CD ORDER BY EDW_END_DTTM DESC) = 1
) LKP ON LKP.ASSET_HOST_ID_VAL = exp_all_sources.in_fixedid AND LKP.PRTY_ASSET_SBTYPE_CD = exp_all_sources.out_prty_asset_sbtype_cd AND LKP.PRTY_ASSET_CLASFCN_CD = exp_all_sources.out_class_cd
QUALIFY RNK = 1
);


-- Component exp_data_transformations, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_data_transformations AS
(
SELECT
LKP_CLM.CLM_ID as CLM_ID,
LKP_PRTY_ASSET_ID.PRTY_ASSET_ID as src_PA_PRTY_ASSET_ID,
exp_all_sources.UPDATETIME as UPDATETIME,
CASE WHEN exp_all_sources.UPDATETIME IS NULL THEN CURRENT_TIMESTAMP ELSE exp_all_sources.UPDATETIME END as in_PRTY_ASSET_SALV_STS_STRT_DT,
in_PRTY_ASSET_SALV_STS_STRT_DT as o_PRTY_ASSET_SALV_STS_STRT_DT,
CASE WHEN exp_all_sources.STATUS IS NULL THEN ''UNK'' ELSE exp_all_sources.STATUS END as out_status,
:PRCS_ID as PRCS_ID,
CURRENT_TIMESTAMP as in_EDW_STRT_DTTM,
to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as in_EDW_END_DTTM,
exp_all_sources.salvagestatus_id as salvagestatus_id,
CASE WHEN exp_all_sources.STATUS = ''open'' THEN exp_all_sources.CreateTime ELSE ( CASE WHEN exp_all_sources.STATUS = ''closed'' THEN exp_all_sources.UPDATETIME ELSE NULL END ) END as o_CreateTime,
TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as o_PRTY_ASSET_SALV_STS_END_DT,
exp_all_sources.source_record_id
FROM
exp_all_sources
INNER JOIN LKP_CLM ON exp_all_sources.source_record_id = LKP_CLM.source_record_id
INNER JOIN LKP_PRTY_ASSET_ID ON LKP_CLM.source_record_id = LKP_PRTY_ASSET_ID.source_record_id
);


-- Component exp_SrcFields, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_SrcFields AS
(
SELECT
exp_data_transformations.src_PA_PRTY_ASSET_ID as in_PRTY_ASSET_ID,
exp_data_transformations.out_status as in_PRTY_ASSET_SALV_STS_CD,
exp_data_transformations.o_CreateTime as in_PRTY_ASSET_SALV_STS_STRT_DT,
exp_data_transformations.o_PRTY_ASSET_SALV_STS_END_DT as in_PRTY_ASSET_SALV_STS_END_DT,
exp_data_transformations.CLM_ID as in_CLM_ID,
exp_data_transformations.PRCS_ID as in_PRCS_ID,
exp_data_transformations.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_data_transformations.in_EDW_END_DTTM as in_EDW_END_DTTM,
exp_data_transformations.o_PRTY_ASSET_SALV_STS_STRT_DT as in_TRANS_STRT_DTTM,
to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) as in_TRANS_END_DTTM,
exp_data_transformations.source_record_id
FROM
exp_data_transformations
);


-- Component LKP_PRTY_ASSET_SALV_STS, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_PRTY_ASSET_SALV_STS AS
(
SELECT
LKP.PRTY_ASSET_ID,
LKP.PRTY_ASSET_SALV_STS_CD,
LKP.PRTY_ASSET_SALV_STS_STRT_DTTM,
LKP.PRTY_ASSET_SALV_STS_END_DTTM,
LKP.CLM_ID,
LKP.EDW_END_DTTM,
exp_SrcFields.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_SrcFields.source_record_id ORDER BY LKP.PRTY_ASSET_ID asc,LKP.PRTY_ASSET_SALV_STS_CD asc,LKP.PRTY_ASSET_SALV_STS_STRT_DTTM asc,LKP.PRTY_ASSET_SALV_STS_END_DTTM asc,LKP.CLM_ID asc,LKP.EDW_END_DTTM asc) RNK
FROM
exp_SrcFields
LEFT JOIN (
SELECT	PRTY_ASSET_SALV_STS.PRTY_ASSET_SALV_STS_STRT_DTTM as PRTY_ASSET_SALV_STS_STRT_DTTM,
		PRTY_ASSET_SALV_STS.PRTY_ASSET_SALV_STS_END_DTTM as PRTY_ASSET_SALV_STS_END_DTTM,
		PRTY_ASSET_SALV_STS.CLM_ID as CLM_ID,
		PRTY_ASSET_SALV_STS.EDW_END_DTTM as EDW_END_DTTM, 
		PRTY_ASSET_SALV_STS.PRTY_ASSET_ID as PRTY_ASSET_ID,
		PRTY_ASSET_SALV_STS.PRTY_ASSET_SALV_STS_CD as PRTY_ASSET_SALV_STS_CD 
FROM	db_t_prod_core.PRTY_ASSET_SALV_STS 
QUALIFY	ROW_NUMBER() OVER(PARTITION BY PRTY_ASSET_SALV_STS.PRTY_ASSET_ID ,PRTY_ASSET_SALV_STS.CLM_ID
ORDER	BY EDW_END_DTTM desc) = 1
) LKP ON LKP.PRTY_ASSET_ID = exp_SrcFields.in_PRTY_ASSET_ID AND LKP.CLM_ID = exp_SrcFields.in_CLM_ID
QUALIFY RNK = 1
);


-- Component exp_CDC_Check, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_CDC_Check AS
(
SELECT
exp_SrcFields.in_PRTY_ASSET_ID as in_PRTY_ASSET_ID,
exp_SrcFields.in_PRTY_ASSET_SALV_STS_CD as in_PRTY_ASSET_SALV_STS_CD,
exp_SrcFields.in_PRTY_ASSET_SALV_STS_STRT_DT as in_PRTY_ASSET_SALV_STS_STRT_DT,
exp_SrcFields.in_PRTY_ASSET_SALV_STS_END_DT as in_PRTY_ASSET_SALV_STS_END_DT,
exp_SrcFields.in_CLM_ID as in_CLM_ID,
exp_SrcFields.in_PRCS_ID as in_PRCS_ID,
exp_SrcFields.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_SrcFields.in_EDW_END_DTTM as in_EDW_END_DTTM,
exp_SrcFields.in_TRANS_STRT_DTTM as in_TRANS_STRT_DTTM,
exp_SrcFields.in_TRANS_END_DTTM as in_TRANS_END_DTTM,
LKP_PRTY_ASSET_SALV_STS.PRTY_ASSET_ID as lkp_PRTY_ASSET_ID,
LKP_PRTY_ASSET_SALV_STS.PRTY_ASSET_SALV_STS_CD as lkp_PRTY_ASSET_SALV_STS_CD,
LKP_PRTY_ASSET_SALV_STS.PRTY_ASSET_SALV_STS_STRT_DTTM as lkp_PRTY_ASSET_SALV_STS_STRT_DT,
LKP_PRTY_ASSET_SALV_STS.PRTY_ASSET_SALV_STS_END_DTTM as lkp_PRTY_ASSET_SALV_STS_END_DT,
NULL as lkp_EDW_STRT_DTTM,
LKP_PRTY_ASSET_SALV_STS.EDW_END_DTTM as lkp_EDW_END_DTTM,
LKP_PRTY_ASSET_SALV_STS.CLM_ID as lkp_CLM_ID,
MD5 ( TO_CHAR ( exp_SrcFields.in_PRTY_ASSET_SALV_STS_STRT_DT , ''MM/DD/YYYY'' ) || TO_CHAR ( exp_SrcFields.in_PRTY_ASSET_SALV_STS_END_DT , ''MM/DD/YYYY'' ) || ltrim ( rtrim ( exp_SrcFields.in_PRTY_ASSET_SALV_STS_CD ) ) ) as v_SRC_MD5,
MD5 ( TO_CHAR ( LKP_PRTY_ASSET_SALV_STS.PRTY_ASSET_SALV_STS_STRT_DTTM , ''MM/DD/YYYY'' ) || TO_CHAR ( LKP_PRTY_ASSET_SALV_STS.PRTY_ASSET_SALV_STS_END_DTTM , ''MM/DD/YYYY'' ) || ltrim ( rtrim ( LKP_PRTY_ASSET_SALV_STS.PRTY_ASSET_SALV_STS_CD ) ) ) as v_TGT_MD5,
CASE WHEN v_TGT_MD5 IS NULL THEN ''I'' ELSE CASE WHEN v_SRC_MD5 = v_TGT_MD5 THEN ''R'' ELSE ''U'' END END as o_CDC_Check,
exp_SrcFields.source_record_id
FROM
exp_SrcFields
INNER JOIN LKP_PRTY_ASSET_SALV_STS ON exp_SrcFields.source_record_id = LKP_PRTY_ASSET_SALV_STS.source_record_id
);


-- Component rtr_pa_salvage_sts_INS_UPD, Type ROUTER Output Group INS_UPD
CREATE OR REPLACE TEMPORARY TABLE rtr_pa_salvage_sts_INS_UPD AS
SELECT
exp_CDC_Check.in_PRTY_ASSET_ID as in_PRTY_ASSET_ID,
exp_CDC_Check.in_PRTY_ASSET_SALV_STS_CD as in_PRTY_ASSET_SALV_STS_CD,
exp_CDC_Check.in_PRTY_ASSET_SALV_STS_STRT_DT as in_PRTY_ASSET_SALV_STS_STRT_DT,
exp_CDC_Check.in_PRTY_ASSET_SALV_STS_END_DT as in_PRTY_ASSET_SALV_STS_END_DT,
exp_CDC_Check.in_CLM_ID as in_CLM_ID,
exp_CDC_Check.in_PRCS_ID as in_PRCS_ID,
exp_CDC_Check.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_CDC_Check.in_EDW_END_DTTM as in_EDW_END_DTTM,
exp_CDC_Check.in_TRANS_STRT_DTTM as in_TRANS_STRT_DTTM,
exp_CDC_Check.in_TRANS_END_DTTM as in_TRANS_END_DTTM,
exp_CDC_Check.lkp_PRTY_ASSET_ID as lkp_PRTY_ASSET_ID,
exp_CDC_Check.lkp_PRTY_ASSET_SALV_STS_CD as lkp_PRTY_ASSET_SALV_STS_CD,
exp_CDC_Check.lkp_PRTY_ASSET_SALV_STS_STRT_DT as lkp_PRTY_ASSET_SALV_STS_STRT_DT,
exp_CDC_Check.lkp_PRTY_ASSET_SALV_STS_END_DT as lkp_PRTY_ASSET_SALV_STS_END_DT,
exp_CDC_Check.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_CDC_Check.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_CDC_Check.lkp_CLM_ID as lkp_CLM_ID,
exp_CDC_Check.o_CDC_Check as o_CDC_CHECK,
exp_CDC_Check.source_record_id
FROM
exp_CDC_Check
WHERE exp_CDC_Check.in_PRTY_ASSET_ID IS NOT NULL and exp_CDC_Check.in_CLM_ID IS NOT NULL and ( ( exp_CDC_Check.o_CDC_Check = ''I'' OR exp_CDC_Check.lkp_EDW_END_DTTM != TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ) OR ( exp_CDC_Check.o_CDC_Check = ''U'' AND exp_CDC_Check.lkp_EDW_END_DTTM = TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ) );


-- Component upd_prty_salv_sts_ins, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_prty_salv_sts_ins AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_pa_salvage_sts_INS_UPD.in_PRTY_ASSET_ID as in_PRTY_ASSET_ID1,
rtr_pa_salvage_sts_INS_UPD.in_PRTY_ASSET_SALV_STS_CD as in_PRTY_ASSET_SALV_STS_CD1,
rtr_pa_salvage_sts_INS_UPD.in_PRTY_ASSET_SALV_STS_STRT_DT as in_PRTY_ASSET_SALV_STS_STRT_DT1,
rtr_pa_salvage_sts_INS_UPD.in_PRTY_ASSET_SALV_STS_END_DT as in_PRTY_ASSET_SALV_STS_END_DT1,
rtr_pa_salvage_sts_INS_UPD.in_CLM_ID as in_CLM_ID1,
rtr_pa_salvage_sts_INS_UPD.in_PRCS_ID as in_PRCS_ID1,
rtr_pa_salvage_sts_INS_UPD.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM1,
rtr_pa_salvage_sts_INS_UPD.in_EDW_END_DTTM as in_EDW_END_DTTM1,
rtr_pa_salvage_sts_INS_UPD.in_TRANS_STRT_DTTM as in_TRANS_STRT_DTTM1,
rtr_pa_salvage_sts_INS_UPD.in_TRANS_END_DTTM as in_TRANS_END_DTTM1,
0 as UPDATE_STRATEGY_ACTION,
rtr_pa_salvage_sts_INS_UPD.source_record_id as source_record_id
FROM
rtr_pa_salvage_sts_INS_UPD
);


-- Component exp_prty_asset_salv_sts_ins, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_prty_asset_salv_sts_ins AS
(
SELECT
upd_prty_salv_sts_ins.in_PRTY_ASSET_ID1 as in_PRTY_ASSET_ID1,
upd_prty_salv_sts_ins.in_PRTY_ASSET_SALV_STS_CD1 as in_PRTY_ASSET_SALV_STS_CD1,
upd_prty_salv_sts_ins.in_PRTY_ASSET_SALV_STS_STRT_DT1 as in_PRTY_ASSET_SALV_STS_STRT_DT1,
upd_prty_salv_sts_ins.in_PRTY_ASSET_SALV_STS_END_DT1 as in_PRTY_ASSET_SALV_STS_END_DT1,
upd_prty_salv_sts_ins.in_CLM_ID1 as in_CLM_ID1,
upd_prty_salv_sts_ins.in_PRCS_ID1 as in_PRCS_ID1,
upd_prty_salv_sts_ins.in_EDW_STRT_DTTM1 as in_EDW_STRT_DTTM1,
upd_prty_salv_sts_ins.in_EDW_END_DTTM1 as in_EDW_END_DTTM1,
upd_prty_salv_sts_ins.in_TRANS_STRT_DTTM1 as in_TRANS_STRT_DTTM1,
upd_prty_salv_sts_ins.in_TRANS_END_DTTM1 as in_TRANS_END_DTTM1,
upd_prty_salv_sts_ins.source_record_id
FROM
upd_prty_salv_sts_ins
);


-- Component PRTY_ASSET_SALV_STS_INS, Type TARGET 
INSERT INTO DB_T_PROD_CORE.PRTY_ASSET_SALV_STS
(
PRTY_ASSET_ID,
PRTY_ASSET_SALV_STS_CD,
PRTY_ASSET_SALV_STS_STRT_DTTM,
PRTY_ASSET_SALV_STS_END_DTTM,
CLM_ID,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM,
TRANS_END_DTTM
)
SELECT
exp_prty_asset_salv_sts_ins.in_PRTY_ASSET_ID1 as PRTY_ASSET_ID,
exp_prty_asset_salv_sts_ins.in_PRTY_ASSET_SALV_STS_CD1 as PRTY_ASSET_SALV_STS_CD,
exp_prty_asset_salv_sts_ins.in_PRTY_ASSET_SALV_STS_STRT_DT1 as PRTY_ASSET_SALV_STS_STRT_DTTM,
exp_prty_asset_salv_sts_ins.in_PRTY_ASSET_SALV_STS_END_DT1 as PRTY_ASSET_SALV_STS_END_DTTM,
exp_prty_asset_salv_sts_ins.in_CLM_ID1 as CLM_ID,
exp_prty_asset_salv_sts_ins.in_PRCS_ID1 as PRCS_ID,
exp_prty_asset_salv_sts_ins.in_EDW_STRT_DTTM1 as EDW_STRT_DTTM,
exp_prty_asset_salv_sts_ins.in_EDW_END_DTTM1 as EDW_END_DTTM,
exp_prty_asset_salv_sts_ins.in_TRANS_STRT_DTTM1 as TRANS_STRT_DTTM,
exp_prty_asset_salv_sts_ins.in_TRANS_END_DTTM1 as TRANS_END_DTTM
FROM
exp_prty_asset_salv_sts_ins;


-- Component PRTY_ASSET_SALV_STS_INS, Type Post SQL 
UPDATE  db_t_prod_core.PRTY_ASSET_SALV_STS  
SET EDW_END_DTTM=A.LEAD1,

TRANS_END_DTTM=A.LEAD2
FROM 

(SELECT	DISTINCT PRTY_ASSET_ID,CLM_ID,TRANS_STRT_DTTM,EDW_STRT_DTTM,

MAX(EDW_STRT_DTTM) OVER (PARTITION BY PRTY_ASSET_ID,CLM_ID ORDER BY EDW_STRT_DTTM ASC ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) - INTERVAL ''1 SECOND'' 

 AS LEAD1,

MAX(TRANS_STRT_DTTM) OVER (PARTITION BY PRTY_ASSET_ID,CLM_ID  ORDER BY TRANS_STRT_DTTM ASC ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) - INTERVAL ''1 SECOND'' 

 AS LEAD2

FROM db_t_prod_core.PRTY_ASSET_SALV_STS 

 ) A
WHERE  

PRTY_ASSET_SALV_STS.EDW_STRT_DTTM = A.EDW_STRT_DTTM 

AND PRTY_ASSET_SALV_STS.TRANS_STRT_DTTM = A.TRANS_STRT_DTTM 

AND PRTY_ASSET_SALV_STS.PRTY_ASSET_ID=A.PRTY_ASSET_ID 

AND PRTY_ASSET_SALV_STS.CLM_ID=A.CLM_ID 

AND CAST(PRTY_ASSET_SALV_STS.EDW_END_DTTM AS DATE) = ''9999-12-31''

AND LEAD1 IS NOT NULL 

AND LEAD2 IS NOT NULL
;


END; ';