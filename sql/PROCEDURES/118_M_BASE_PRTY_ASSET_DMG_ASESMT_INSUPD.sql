-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_PRTY_ASSET_DMG_ASESMT_INSUPD("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' BEGIN 
SET PRCS_ID=''1'';
-- Component SQ_cc_vehicle, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_cc_vehicle AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as CLASS_CD,
$2 as Vin,
$3 as DMG_CD,
$4 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select distinct ''MV'' as "prty_asset_classification",

case when cc_vehicle.PolicySystemId is not null then SUBSTRING(cc_vehicle.policysystemid,position ('':'' in cc_vehicle.policysystemid)+1 ,LENGTH(cc_vehicle.policysystemid))

when (cc_vehicle.PolicySystemId is null and cc_vehicle.Vin is not null) then  ''VIN:'' || cc_vehicle.vin

when (cc_vehicle.PolicySystemId is null and cc_vehicle.Vin is null and cc_vehicle.LicensePlate is not null) then ''LP:'' || cc_vehicle.licenseplate

when (cc_vehicle.PolicySystemId is null and cc_vehicle.Vin is null and cc_vehicle.LicensePlate is null) then PublicID

end as "Asset_Host_Id_Val"

, ''TotalLoss'' as "DMG_CD"

from DB_T_PROD_STAG.cc_vehicle

where cc_vehicle.policysystemid is null



union



select distinct ''MV'' as "prty_asset_classification",

case when cc_vehicle.PolicySystemId is not null then SUBSTRING(cc_vehicle.policysystemid,position ('':'' in cc_vehicle.policysystemid)+1 ,LENGTH(cc_vehicle.policysystemid))

when (cc_vehicle.PolicySystemId is null and cc_vehicle.Vin is not null) then  ''VIN:'' || cc_vehicle.vin

when (cc_vehicle.PolicySystemId is null and cc_vehicle.Vin is null and cc_vehicle.LicensePlate is not null) then ''LP:'' || cc_vehicle.licenseplate

when (cc_vehicle.PolicySystemId is null and cc_vehicle.Vin is null and cc_vehicle.LicensePlate is null) then PublicID

end as "Asset_Host_Id_Val"

, ''VehStolenInd'' as "DMG_CD"

from DB_T_PROD_STAG.cc_vehicle

where cc_vehicle.policysystemid is null
) SRC
)
);


-- Component exp_all_sources, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_all_sources AS
(
SELECT
SQ_cc_vehicle.CLASS_CD as CLASS_CD,
SQ_cc_vehicle.Vin as Vin,
SQ_cc_vehicle.DMG_CD as DMG_CD,
''MVEH'' as out_prty_asset_sbtype_cd,
SQ_cc_vehicle.source_record_id
FROM
SQ_cc_vehicle
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
FROM PRTY_ASSET 
QUALIFY ROW_NUMBER() OVER(PARTITION BY  ASSET_HOST_ID_VAL,PRTY_ASSET_SBTYPE_CD,PRTY_ASSET_CLASFCN_CD ORDER BY EDW_END_DTTM DESC) = 1
) LKP ON LKP.ASSET_HOST_ID_VAL = exp_all_sources.Vin AND LKP.PRTY_ASSET_SBTYPE_CD = exp_all_sources.out_prty_asset_sbtype_cd AND LKP.PRTY_ASSET_CLASFCN_CD = exp_all_sources.CLASS_CD
QUALIFY RNK = 1
);


-- Component LKP_PA_DMG_ASESMT_ID, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_PA_DMG_ASESMT_ID AS
(
SELECT
LKP.PRTY_ASSET_ID,
LKP.PRTY_ASSET_DMG_RPORT_DT,
LKP.PRCS_ID,
LKP_PRTY_ASSET_ID.source_record_id,
ROW_NUMBER() OVER(PARTITION BY LKP_PRTY_ASSET_ID.source_record_id ORDER BY LKP.PRTY_ASSET_ID asc,LKP.PRTY_ASSET_DMG_CD asc,LKP.PRTY_ASSET_DMG_RPORT_DT asc,LKP.PRCS_ID asc) RNK
FROM
LKP_PRTY_ASSET_ID
LEFT JOIN (
SELECT
PRTY_ASSET_ID,
PRTY_ASSET_DMG_CD,
PRTY_ASSET_DMG_RPORT_DT,
PRCS_ID
FROM PRTY_ASSET_DMG_ASESMT
) LKP ON LKP.PRTY_ASSET_ID = LKP_PRTY_ASSET_ID.PRTY_ASSET_ID
QUALIFY RNK = 1
);


-- Component exp_data_transformation, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_data_transformation AS
(
SELECT
LKP_PA_DMG_ASESMT_ID.PRTY_ASSET_ID as PRTY_ASSET_ID,
exp_all_sources.DMG_CD as DMG_CD,
LKP_PRTY_ASSET_ID.PRTY_ASSET_ID as PRTY_ASSET_ID1,
exp_all_sources.source_record_id
FROM
exp_all_sources
INNER JOIN LKP_PRTY_ASSET_ID ON exp_all_sources.source_record_id = LKP_PRTY_ASSET_ID.source_record_id
INNER JOIN LKP_PA_DMG_ASESMT_ID ON LKP_PRTY_ASSET_ID.source_record_id = LKP_PA_DMG_ASESMT_ID.source_record_id
);


-- Component exp_data_transformations, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_data_transformations AS
(
SELECT
exp_data_transformation.PRTY_ASSET_ID as old_PRTY_ASSET_ID,
CURRENT_TIMESTAMP as PRTY_ASSET_DMG_RPORT_DT,
:PRCS_ID as PRCS_ID,
CASE WHEN exp_data_transformation.PRTY_ASSET_ID IS NULL THEN ''Y'' ELSE ''N'' END as out_ins_upd_flag,
exp_data_transformation.PRTY_ASSET_ID1 as new_PRTY_ASSET_ID1,
CASE WHEN exp_data_transformation.DMG_CD IS NULL THEN ''UNK'' ELSE exp_data_transformation.DMG_CD END as out_tgt_ref_type_cd,
exp_data_transformation.source_record_id
FROM
exp_data_transformation
);


-- Component rtr_pa_dmg_asesmt_INSERT, Type ROUTER Output Group INSERT
SELECT
exp_data_transformations.new_PRTY_ASSET_ID1 as new_PRTY_ASSET_ID,
exp_data_transformations.out_tgt_ref_type_cd as TGT_REF_TYPE_CD,
exp_data_transformations.old_PRTY_ASSET_ID as old_PRTY_ASSET_ID,
exp_data_transformations.PRTY_ASSET_DMG_RPORT_DT as PRTY_ASSET_DMG_RPORT_DT,
exp_data_transformations.PRCS_ID as PRCS_ID,
exp_data_transformations.out_ins_upd_flag as in_ins_upd_flag,
exp_data_transformations.source_record_id
FROM
exp_data_transformations
WHERE CASE WHEN exp_data_transformations.old_PRTY_ASSET_ID IS NULL THEN TRUE ELSE FALSE END and CASE WHEN exp_data_transformations.new_PRTY_ASSET_ID1 IS NOT NULL THEN TRUE ELSE FALSE END;


-- Component rtr_pa_dmg_asesmt_UPDATE, Type ROUTER Output Group UPDATE
SELECT
exp_data_transformations.new_PRTY_ASSET_ID1 as new_PRTY_ASSET_ID,
exp_data_transformations.out_tgt_ref_type_cd as TGT_REF_TYPE_CD,
exp_data_transformations.old_PRTY_ASSET_ID as old_PRTY_ASSET_ID,
exp_data_transformations.PRTY_ASSET_DMG_RPORT_DT as PRTY_ASSET_DMG_RPORT_DT,
exp_data_transformations.PRCS_ID as PRCS_ID,
exp_data_transformations.out_ins_upd_flag as in_ins_upd_flag,
exp_data_transformations.source_record_id
FROM
exp_data_transformations
WHERE CASE WHEN exp_data_transformations.old_PRTY_ASSET_ID IS NOT NULL THEN TRUE ELSE FALSE END;


-- Component upd_pa_dmg_asesmt_upd, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_pa_dmg_asesmt_upd AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_pa_dmg_asesmt_UPDATE.TGT_REF_TYPE_CD as TGT_REF_TYPE_CD3,
rtr_pa_dmg_asesmt_UPDATE.old_PRTY_ASSET_ID as PRTY_ASSET_ID3,
rtr_pa_dmg_asesmt_UPDATE.PRTY_ASSET_DMG_RPORT_DT as PRTY_ASSET_DMG_RPORT_DT3,
rtr_pa_dmg_asesmt_UPDATE.PRCS_ID as PRCS_ID3,
1 as UPDATE_STRATEGY_ACTION
FROM
rtr_pa_dmg_asesmt_UPDATE
);


-- Component upd_pa_dmg_asesmt_ins, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_pa_dmg_asesmt_ins AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_pa_dmg_asesmt_INSERT.TGT_REF_TYPE_CD as TGT_REF_TYPE_CD1,
rtr_pa_dmg_asesmt_INSERT.new_PRTY_ASSET_ID as PRTY_ASSET_ID1,
rtr_pa_dmg_asesmt_INSERT.PRTY_ASSET_DMG_RPORT_DT as PRTY_ASSET_DMG_RPORT_DT1,
rtr_pa_dmg_asesmt_INSERT.PRCS_ID as PRCS_ID1,
0 as UPDATE_STRATEGY_ACTION
FROM
rtr_pa_dmg_asesmt_INSERT
);


-- Component exp_pass_to_tgt2, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_tgt2 AS
(
SELECT
upd_pa_dmg_asesmt_upd.TGT_REF_TYPE_CD3 as TGT_REF_TYPE_CD3,
upd_pa_dmg_asesmt_upd.PRTY_ASSET_ID3 as PRTY_ASSET_ID3,
upd_pa_dmg_asesmt_upd.PRTY_ASSET_DMG_RPORT_DT3 as PRTY_ASSET_DMG_RPORT_DT3,
upd_pa_dmg_asesmt_upd.PRCS_ID3 as PRCS_ID3,
upd_pa_dmg_asesmt_upd.source_record_id
FROM
upd_pa_dmg_asesmt_upd
);


-- Component exp_pass_to_tgt, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_tgt AS
(
SELECT
upd_pa_dmg_asesmt_ins.TGT_REF_TYPE_CD1 as TGT_REF_TYPE_CD1,
upd_pa_dmg_asesmt_ins.PRTY_ASSET_ID1 as PRTY_ASSET_ID1,
upd_pa_dmg_asesmt_ins.PRTY_ASSET_DMG_RPORT_DT1 as PRTY_ASSET_DMG_RPORT_DT1,
upd_pa_dmg_asesmt_ins.PRCS_ID1 as PRCS_ID1,
upd_pa_dmg_asesmt_ins.source_record_id
FROM
upd_pa_dmg_asesmt_ins
);


-- Component PRTY_ASSET_DMG_ASESMT1, Type TARGET 
MERGE INTO PRTY_ASSET_DMG_ASESMT
USING exp_pass_to_tgt2 ON (PRTY_ASSET_DMG_ASESMT.PRTY_ASSET_ID = exp_pass_to_tgt2.PRTY_ASSET_ID3)
WHEN MATCHED THEN UPDATE
SET
--PRTY_ASSET_ID = exp_pass_to_tgt2.PRTY_ASSET_ID3,
PRTY_ASSET_DMG_CD = exp_pass_to_tgt2.TGT_REF_TYPE_CD3,
PRTY_ASSET_DMG_RPORT_DT = exp_pass_to_tgt2.PRTY_ASSET_DMG_RPORT_DT3,
PRCS_ID = exp_pass_to_tgt2.PRCS_ID3;


-- Component PRTY_ASSET_DMG_ASESMT, Type TARGET 
INSERT INTO PRTY_ASSET_DMG_ASESMT
(
PRTY_ASSET_ID,
PRTY_ASSET_DMG_CD,
PRTY_ASSET_DMG_RPORT_DT,
PRCS_ID
)
SELECT
exp_pass_to_tgt.PRTY_ASSET_ID1 as PRTY_ASSET_ID,
exp_pass_to_tgt.TGT_REF_TYPE_CD1 as PRTY_ASSET_DMG_CD,
exp_pass_to_tgt.PRTY_ASSET_DMG_RPORT_DT1 as PRTY_ASSET_DMG_RPORT_DT,
exp_pass_to_tgt.PRCS_ID1 as PRCS_ID
FROM
exp_pass_to_tgt;


END; ';