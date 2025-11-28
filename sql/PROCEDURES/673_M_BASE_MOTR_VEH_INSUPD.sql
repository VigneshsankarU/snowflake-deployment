-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_MOTR_VEH_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
 DECLARE 

P_DEFAULT_STR_CD varchar;

run_id varchar;
start_dttm timestamp;
end_dttm timestamp;
prcs_id int;


BEGIN 
 run_id :=   (SELECT run_id   FROM control_run_id where upper(worklet_name) = upper(:worklet_name) order by insert_ts desc limit 1);   
 END_DTTM:=   (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''END_DTTM'' order by insert_ts desc limit 1);
 START_DTTM:=     (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''START_DTTM'' order by insert_ts desc limit 1);
 PRCS_ID:=     (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PRCS_ID'' order by insert_ts desc limit 1);
P_DEFAULT_STR_CD :=  (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''P_DEFAULT_STR_CD'' order by insert_ts desc limit 1);

--P_DEFAULT_STR_CD:=''z'';

-- Component LKP_TERADATA_ETL_REF_XLAT, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''MOTR_VEH_TYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''pctl_vehicletype.typecode'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''GW'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT3, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT3 AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''VEH_MFGR_TYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''cctl_vehiclemanufacturer.Typecode'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''GW'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


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

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM in ( ''derived'' ,''pcx_holineschcovitemcov_alfa.ChoiceTerm1'', ''contentlineitemschedule.typecode'')

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

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''PRTY_ASSET_SBTYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_BODY_TYPE, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_BODY_TYPE AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''BODY_TYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM in ( ''pctl_bodytype.typecode'' ,''pc_personalvehicle.bodytype_alfa'')

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

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''SRC_SYS'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_XMISN_TYPE, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_XMISN_TYPE AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''XMISN_TYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM=''pctl_motortype_alfa.typecode''

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''GW'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component sq_pc_personalvehicle, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_pc_personalvehicle AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as FixedID,
$2 as sb_type_cd,
$3 as CLASS_CD,
$4 as SYS_SRC_CD,
$5 as Bodytype,
$6 as Yearbuilt,
$7 as Model,
$8 as Vin,
$9 as VehTypecode,
$10 as Manufacturer,
$11 as Createtime,
$12 as End_dt,
$13 as veh_mf_name,
$14 as mf_name_frdm_ind,
$15 as make_name_frdm_ind,
$16 as Trans_strt_dttm,
$17 as Name,
$18 as HorsePower_alfa,
$19 as rnk,
$20 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select 

id,type_code,classification_code,src_cd,body_type,Yearbuilt,Model,Vin,vehicle_type,maufacture_code,createtime,end_dt,veh_mf_name,mf_name_frdm_ind,make_name_frdm_ind,Trans_Strt_Dttm,replace(name,''/'','''') as name,cast (coalesce(HorsePower_alfa,0) as  integer) as HorsePower_alfa,

Rank()  OVER(PARTITION BY id,type_code,classification_code ORDER BY Trans_Strt_Dttm desc )  as rnk  

from (

select	distinct 

cast(fixedid_stg  as varchar(100))as id 

,''PRTY_ASSET_SBTYPE4''  as type_code ,

cast (''PRTY_ASSET_CLASFCN3'' as varchar (60)) as classification_code,

		

''SRC_SYS4'' as src_cd,

/* pctl_BodyType.TYPECODE as body_type , */
cast (pc_personalvehicle.BodyType_alfa_stg as  varchar (60))  as body_type,

pc_personalvehicle.Year_stg as yearbuilt,

pc_personalvehicle.Model_stg as model,

pc_personalvehicle.Vin_stg as vin,

pctl_vehicletype.typecode_stg as vehicle_type,

pc_personalvehicle.make_stg as maufacture_code,

cast(pc_personalvehicle.createtime_stg as date ) createtime,

cast(''01/01/1900'' as date ) as end_dt,

cast(make_stg as varchar(100)) as veh_mf_name,

pc_personalvehicle.IsUserEnteredmake_alfa_stg as mf_name_frdm_ind,

pc_personalvehicle.IsUserEnteredModel_alfa_stg as make_name_frdm_ind,

pc_personalvehicle.updatetime_stg as Trans_Strt_Dttm,

pctl_motortype_alfa.name_stg as name,

pc_personalvehicle.HorsePower_alfa_stg as horsepower_alfa

from	(

	SELECT	pc_personalvehicle.FixedID_stg, pc_personalvehicle.BodyType_alfa_stg, pc_personalvehicle.Year_stg,

pc_personalvehicle.Model_stg,

pc_personalvehicle.Vin_stg, pc_personalvehicle.make_stg, pc_personalvehicle.createtime_stg, 

pc_personalvehicle.IsUserEnteredmake_alfa_stg,

pc_personalvehicle.IsUserEnteredModel_alfa_stg,

pc_personalvehicle.updatetime_stg,

pc_personalvehicle.HorsePower_alfa_stg, pc_personalvehicle.GarageLocation_stg, pc_personalvehicle.BranchID_stg, 

pc_personalvehicle.ExpirationDate_stg, pc_personalvehicle.VehicleType_stg, pc_personalvehicle.MotorType_alfa_stg,

	(:start_dttm) as start_dttm,

	(:end_dttm) as end_dttm



	FROM

	 DB_T_PROD_STAG.pc_personalvehicle

	 left outer join DB_T_PROD_STAG.pc_policylocation 

		on pc_personalvehicle.GarageLocation_stg = pc_policylocation.id_stg 

	 left outer join DB_T_PROD_STAG.pc_TerritoryCode 

		on pc_policylocation.id_stg=pc_TerritoryCode.PolicyLocation_stg

	 left outer join DB_T_PROD_STAG.pctl_territorycode 

		on pctl_territorycode.id_stg=pc_territorycode.Subtype_stg

	left outer join DB_T_PROD_STAG.pc_policyperiod 

		on pc_personalvehicle.BranchID_stg=pc_policyperiod.id_stg

	where

	 pc_personalvehicle.updatetime_stg> (:start_dttm)

		and pc_personalvehicle.updatetime_stg <= (:end_dttm)

		and  (pc_personalvehicle.ExpirationDate_stg is null 

or pc_personalvehicle.ExpirationDate_stg >:start_dttm) /* EIM-15097 Added Expiration_date filter */
		

		

) pc_personalvehicle 

left outer join DB_T_PROD_STAG.pctl_vehicletype 

	on pc_personalvehicle.VehicleType_stg = pctl_vehicletype.id_stg 

left outer join DB_T_PROD_STAG.pctl_motortype_alfa 

	on pc_personalvehicle.MotorType_alfa_stg = pctl_motortype_alfa.id_stg 

/* left outer join DB_T_PROD_STAG.pctl_BodyType on pc_personalvehicle.BodyType_stg = pctl_BodyType.id  */
where	fixedid_stg is not null 

	and ExpirationDate_stg is null



/* --------------------------------- */
UNION



select distinct 

cast(fixedid_stg  as varchar(100))as id , 

''PRTY_ASSET_SBTYPE4''  as type_code ,

''PRTY_ASSET_CLASFCN4'' as classification_code, 

''SRC_SYS4'' as src_cd

,cast (null as  varchar (60))  as body_type ,

NULL as Yearbuilt,

cast (null as  varchar (60)) as Model,

cast (null as  varchar (60)) as vin,

cast (null as  varchar (60)) as vehicle_type,

cast (null as  varchar (60)) as maufacture_code,

cast(''01/01/1900'' as date ) as createtime,

cast(''01/01/1900'' as date ) as end_dt,

cast('''' as varchar (100)) as veh_mf_name,

NULL as mf_name_frdm_ind,

NULL as make_name_frdm_ind,

updatetime_stg as Trans_Strt_Dttm,

cast (null as  varchar (60)) as name,

cast (0 as  integer) as HorsePower_alfa

from (

SELECT	pcx_pawatercraftmotor_alfa.FixedID_stg, pcx_pawatercraftmotor_alfa.UpdateTime_stg,

(:start_dttm) as start_dttm,

(:end_dttm) as end_dttm

FROM	DB_T_PROD_STAG.pcx_pawatercraftmotor_alfa

WHERE pcx_pawatercraftmotor_alfa.UpdateTime_stg>(:start_dttm) AND pcx_pawatercraftmotor_alfa.UpdateTime_stg <= (:end_dttm)

) pcx_pawatercraftmotor_alfa

 where fixedid_stg is not null

 

/* -------------------------------- */
UNION



select	distinct 

cast(fixedid_stg  as varchar(100))as id ,

		

''PRTY_ASSET_SBTYPE4''  as type_code ,

''PRTY_ASSET_CLASFCN5'' as classification_code,

		

''SRC_SYS4'' as src_cd

,cast (null as  varchar (60))  as body_type ,

NULL as Yearbuilt,

cast (null as  varchar (60)) as Model,

cast (null as  varchar (60)) as vin,

cast (null as  varchar (60)) as vehicle_type,

cast (null as  varchar (60)) as maufacture_code,

cast(''01/01/1900'' as date ) as createtime,

cast(''01/01/1900'' as date ) as end_dt,

cast('''' as varchar (100)) as veh_mf_name,

NULL as mf_name_frdm_ind,

NULL as make_name_frdm_ind,

updatetime_stg as Trans_Strt_Dttm,

cast (null as  varchar (60)) as name,

cast (0 as  integer) as HorsePower_alfa

from	

(



	SELECT	pcx_pawatercrafttrailer_alfa.FixedID_stg, pcx_pawatercrafttrailer_alfa.UpdateTime_stg,

	(:start_dttm) as start_dttm,

	(:end_dttm) as end_dttm 

	FROM DB_T_PROD_STAG.pcx_pawatercrafttrailer_alfa

	WHERE

	pcx_pawatercrafttrailer_alfa.UpdateTime_stg>(:start_dttm) 

		AND pcx_pawatercrafttrailer_alfa.UpdateTime_stg <= (:end_dttm)

) pcx_pawatercrafttrailer_alfa 

where	fixedid_stg is not null



UNION



SELECT DISTINCT 

 CASE 

	WHEN (cc_vehicle.veh_PolicySystemId_stg IS NULL

           AND cc_vehicle.veh_Vin_stg IS NOT NULL) THEN ''VIN:''|| cc_vehicle.veh_Vin_stg

     WHEN (cc_vehicle.veh_PolicySystemId_stg IS NULL

           AND cc_vehicle.veh_Vin_stg IS NULL

           AND cc_vehicle.veh_LicensePlate_stg IS NOT NULL) THEN ''LP:'' || cc_vehicle.veh_LicensePlate_stg

     WHEN (cc_vehicle.veh_PolicySystemId_stg IS NULL

           AND cc_vehicle.veh_Vin_stg IS NULL

           AND cc_vehicle.veh_LicensePlate_stg IS NULL) THEN cc_vehicle.veh_PublicID_stg

	END AS id,

 ''PRTY_ASSET_SBTYPE4'' AS type_code,

 ''PRTY_ASSET_CLASFCN3'' AS classification_code,

 ''SRC_SYS6'' AS src_cd,

 CAST (NULL AS varchar (60)) AS body_type,

      cc_vehicle.veh_Year_stg as yearbuilt,

      cc_vehicle.veh_Model_stg as model,

      cc_vehicle.veh_Vin_stg as vin,

      cctl_vehiclestyle.typecode_stg AS vehicle_type,

      cc_vehicle.veh_Make_stg AS maufacture_code,

      cast(''01/01/1900'' AS date ) AS createtime,

      cast(''01/01/1900'' AS date ) AS end_dt,

      veh_make_stg AS veh_mf_name,

      NULL AS mf_name_frdm_ind,

      NULL AS make_name_frdm_ind,

      cc_incident.incd_updatetime_stg AS Trans_Strt_Dttm,

      CAST (NULL AS varchar (60)) AS name,

           CAST (0 AS integer) AS HorsePower_alfa

FROM

  (SELECT DISTINCT cc_incident.RecovInd_stg as incd_RecovInd_stg,

                   cc_incident.TotalLossPoints_stg as incd_TotalLossPoints_stg,

                   cc_incident.LocationInd_stg as incd_LocationInd_stg,

                   cc_incident.NumStories_stg as incd_NumStories_stg,

                   cc_incident.VehRecoveredInd_alfa_stg as incd_VehRecoveredInd_alfa_stg,

                   cc_incident.AirbagsMissing_stg as incd_AirbagsMissing_stg,

                   cc_incident.VehicleTitleRecvd_stg as incd_VehicleTitleRecvd_stg,

                   cc_incident.CarrierCompensated_stg as incd_CarrierCompensated_stg,

                   cc_incident.ExtWallMat_stg as incd_ExtWallMat_stg,

                   cc_incident.FireBurnDash_stg as incd_FireBurnDash_stg,

                   cc_incident.GeneralInjuryType_stg as incd_GeneralInjuryType_stg,

                   cc_incident.FireProtectionAvailable_stg as incd_FireProtectionAvailable_stg,

                   cc_incident.RecovClassType_stg as incd_RecovClassType_stg,

                   cc_incident.CreateUserID_stg as incd_CreateUserID_stg,

                   cc_incident.DriverRelation_stg as incd_DriverRelation_stg,

                   cc_incident.IsAMinor_alfa_stg as incd_IsAMinor_alfa_stg,

                   cc_incident.RentalAgency_stg as incd_RentalAgency_stg,

                   cc_incident.salvageproceeds_stg as incd_salvageproceeds_stg,

                   cc_incident.DebrisRemovalInd_stg as incd_DebrisRemovalInd_stg,

                   cc_incident.UpdateUserID_stg as incd_UpdateUserID_stg,

                   cc_incident.LossArea_stg as incd_LossArea_stg,

                   cc_incident.VehicleTitleReqd_stg as incd_VehicleTitleReqd_stg,

                   cc_incident.AssessmentTargetCloseDate_stg as incd_AssessmentTargetCloseDate_stg,

                   cc_incident.FencesDamaged_stg as incd_FencesDamaged_stg,

                   cc_incident.BodyShopSelected_stg as incd_BodyShopSelected_stg,

                   cc_incident.LossDesc_stg as incd_LossDesc_stg,

                   cc_incident.VehicleParked_stg as incd_VehicleParked_stg,

                   cc_incident.FireBurnWindshield_stg as incd_FireBurnWindshield_stg,

                   cc_incident.VehicleDriveable_stg as incd_VehicleDriveable_stg,

                   cc_incident.EquipmentFailure_stg as incd_EquipmentFailure_stg,

                   cc_incident.NumberOfPeopleOnPolicy_stg as incd_NumberOfPeopleOnPolicy_stg,

                   cc_incident.ReturnToWorkValid_stg as incd_ReturnToWorkValid_stg,

                   cc_incident.TripRUID_stg as incd_TripRUID_stg,

                   cc_incident.FloodSaltWater_stg as incd_FloodSaltWater_stg,

                   cc_incident.SalvageYard_alfa_stg as incd_SalvageYard_alfa_stg,

                   cc_incident.Speed_stg as incd_Speed_stg,

                   cc_incident.SalvageStatus_alfa_stg as incd_SalvageStatus_alfa_stg,

                   cc_incident.AssessmentName_stg as incd_AssessmentName_stg,

                   cc_incident.CitationIssued_stg as incd_CitationIssued_stg,

                   cc_incident.SalvagePrep_stg as incd_SalvagePrep_stg,

                   cc_incident.SalvageTow_stg as incd_SalvageTow_stg,

                   cc_incident.ClaimID_stg as incd_ClaimID_stg,

                   cc_incident.Impairment_stg as incd_Impairment_stg,

                   cc_incident.MovePermission_stg as incd_MovePermission_stg,

                   cc_incident.YearBuilt_stg as incd_YearBuilt_stg,

                   cc_incident.DwellingACV_alfa_stg as incd_DwellingACV_alfa_stg, 

                   cc_incident.WaterLevelSeats_stg as incd_WaterLevelSeats_stg,

                   cc_incident.Mileage100K_stg as incd_Mileage100K_stg,

                   cc_incident.VehicleType_stg as incd_VehicleType_stg,

                   cc_incident.TotalLoss_stg as incd_TotalLoss_stg,

                   cc_incident.ArchivePartition_stg as incd_ArchivePartition_stg,

                   cc_incident.StorageFclty_stg as incd_StorageFclty_stg,

                   cc_incident.BaggageType_stg as incd_BaggageType_stg,

                   cc_incident.RecovState_stg as incd_RecovState_stg,

                   cc_incident.AirbagsDeployed_stg as incd_AirbagsDeployed_stg,

                   cc_incident.AttorneyRepresented_alfa_stg as incd_AttorneyRepresented_alfa_stg,

                   cc_incident.MinorOnPolicy_stg as incd_MinorOnPolicy_stg,

                   cc_incident.DriverRelToOwner_stg as incd_DriverRelToOwner_stg,

                   cc_incident.OdomRead_stg as incd_OdomRead_stg,

                   cc_incident.Extrication_stg as incd_Extrication_stg,

                   cc_incident.ReturnToModWorkActual_stg as incd_ReturnToModWorkActual_stg,

                   cc_incident.SalvageNet_stg as incd_SalvageNet_stg,

                   cc_incident.IncludeContentLineItems_stg as incd_IncludeContentLineItems_stg,

                   cc_incident.LocationAddress_stg as incd_LocationAddress_stg,

                   cc_incident.IncludeLineItems_stg as incd_IncludeLineItems_stg,

                   cc_incident.RecovDate_stg as incd_RecovDate_stg,

                   cc_incident.HitAndRun_stg as incd_HitAndRun_stg,

                   cc_incident.CarrierCompensatedAmount_stg as incd_CarrierCompensatedAmount_stg,

                   cc_incident.ClassType_stg as incd_ClassType_stg,

                   cc_incident.DateDwellSalvageAssign_alfa_stg as incd_DateDwellSalvageAssign_alfa_stg,

                   cc_incident.ReturnToModWorkDate_stg as incd_ReturnToModWorkDate_stg,

                   cc_incident.AntiThftInd_stg as incd_AntiThftInd_stg,

                   cc_incident.StorageFeeAmt_stg as incd_StorageFeeAmt_stg,

                   cc_incident.PercentageDrivenByMinor_stg as incd_PercentageDrivenByMinor_stg,

                   cc_incident.MedicalTreatmentType_stg as incd_MedicalTreatmentType_stg,

                   cc_incident.RentalRequired_stg as incd_RentalRequired_stg,

                   cc_incident.HitAndRunReported_alfa_stg as incd_HitAndRunReported_alfa_stg,

                   cc_incident.BaggageMissingFrom_stg as incd_BaggageMissingFrom_stg,

                   cc_incident.VehiclePolStatus_stg as incd_VehiclePolStatus_stg,

                   cc_incident.RecovCondType_stg as incd_RecovCondType_stg,

                   cc_incident.VehicleAge5Years_stg as incd_VehicleAge5Years_stg,

                   cc_incident.PersonalPropertySchedule_alfa_stg as incd_PersonalPropertySchedule_alfa_stg,

                   cc_incident.RentalReserveNo_stg as incd_RentalReserveNo_stg,

                   cc_incident.YearsInHome_stg as incd_YearsInHome_stg,

                   cc_incident.VehicleUseReason_stg as incd_VehicleUseReason_stg,

                   cc_incident.EstRepairCost_stg as incd_EstRepairCost_stg,

                   cc_incident.VehicleLocation_stg as incd_VehicleLocation_stg,

                   cc_incident.BaggageRecoveredOn_stg as incd_BaggageRecoveredOn_stg,

                   cc_incident.RecoveryLocationID_stg as incd_RecoveryLocationID_stg,

                   cc_incident.Appraisal_stg as incd_Appraisal_stg,

                   cc_incident.ReturnToWorkDate_stg as incd_ReturnToWorkDate_stg,

                   cc_incident.SalvageStorage_stg as incd_SalvageStorage_stg,

                   cc_incident.SalvageCompany_stg as incd_SalvageCompany_stg,

                   cc_incident.AssessmentType_stg as incd_AssessmentType_stg,

                   cc_incident.Severity_stg as incd_Severity_stg,

                   cc_incident.OwnerRetainingSalvage_stg as incd_OwnerRetainingSalvage_stg,

                   cc_incident.TotalLoss_alfa_stg as incd_TotalLoss_alfa_stg,

                   cc_incident.FireProtDetails_stg as incd_FireProtDetails_stg,

                   cc_incident.DateVehicleRecovered_stg as incd_DateVehicleRecovered_stg,

                   cc_incident.SprinkRetServ_stg as incd_SprinkRetServ_stg,

                   cc_incident.OccupancyType_stg as incd_OccupancyType_stg,

                   cc_incident.DateSalvageAssigned_stg as incd_DateSalvageAssigned_stg,

                   cc_incident.LossOccured_stg as incd_LossOccured_stg,

                   cc_incident.AlarmType_stg as incd_AlarmType_stg,

                   cc_incident.LossofUse_stg as incd_LossofUse_stg,

                   cc_incident.ClaimIncident_stg as incd_ClaimIncident_stg,

                   cc_incident.StartDate_stg as incd_StartDate_stg,

                   cc_incident.SalvageTitle_stg as incd_SalvageTitle_stg,

                   cc_incident.InspectionRequired_stg as incd_InspectionRequired_stg,

                   cc_incident.updatetime_stg as incd_updatetime_stg,

                   cc_incident.Medicare_alfa_stg as incd_Medicare_alfa_stg,

                   cc_incident.ID_stg as incd_ID_stg,

                   cc_incident.VehLockInd_stg as incd_VehLockInd_stg,

                   cc_incident.DescOther_stg as incd_DescOther_stg,

                   cc_incident.InternalUserID_stg as incd_InternalUserID_stg,

                   cc_incident.AssessmentCloseDate_stg as incd_AssessmentCloseDate_stg,

                   cc_incident.DwellingSalvageNetRecover_alfa_stg as incd_DwellingSalvageNetRecover_alfa_stg,

                   cc_incident.BeanVersion_stg as incd_BeanVersion_stg,

                   cc_incident.MaterialsDamaged_stg as incd_MaterialsDamaged_stg,

                   cc_incident.EstimatesReceived_stg as incd_EstimatesReceived_stg,

                   cc_incident.EstDamageType_stg as incd_EstDamageType_stg,

                   cc_incident.Collision_stg as incd_Collision_stg,

                   cc_incident.RentalBeginDate_stg as incd_RentalBeginDate_stg,

                   cc_incident.EMSInd_stg as incd_EMSInd_stg,

                   cc_incident.AssessmentStatus_stg as incd_AssessmentStatus_stg,

                   cc_incident.AlreadyRepaired_stg as incd_AlreadyRepaired_stg,

                   cc_incident.PublicID_stg as incd_PublicID_stg,

                   cc_incident.VehicleID_stg as incd_VehicleID_stg,

                   cc_incident.ReportedToMedicare_alfa_stg as incd_ReportedToMedicare_alfa_stg,

                   cc_incident.KeysInPossession_alfa_stg as incd_KeysInPossession_alfa_stg,

                   cc_incident.LossEstimate_stg as incd_LossEstimate_stg,

                   cc_incident.DateDwellingSold_alfa_stg as incd_DateDwellingSold_alfa_stg,

                   cc_incident.StorageAccrInd_stg as incd_StorageAccrInd_stg,

                   cc_incident.PropertySize_stg as incd_PropertySize_stg,

                   cc_incident.HazardInvolved_stg as incd_HazardInvolved_stg,

                   cc_incident.AmbulanceUsed_stg as incd_AmbulanceUsed_stg,

                   cc_incident.HICN_alfa_stg as incd_HICN_alfa_stg,

                   cc_incident.WhenToView_stg as incd_WhenToView_stg,

                   cc_incident.AssessmentComment_stg as incd_AssessmentComment_stg,

                   cc_incident.VehicleAge10Years_stg as incd_VehicleAge10Years_stg,

                   cc_incident.SubroPotential_alfa_stg as incd_SubroPotential_alfa_stg,

                   cc_incident.RentalEndDate_stg as incd_RentalEndDate_stg,

                   cc_incident.MealsDays_stg as incd_MealsDays_stg,

                   cc_incident.RelatedTripRUID_stg as incd_RelatedTripRUID_stg,

                   cc_incident.CoverageBasis_alfa_stg as incd_CoverageBasis_alfa_stg,

                   cc_incident.NumberDaysVacant_alfa_stg as incd_NumberDaysVacant_alfa_stg,

                   cc_incident.OwnerRetainDwellSalvage_alfa_stg as incd_OwnerRetainDwellSalvage_alfa_stg,

                   cc_incident.Subtype_stg as incd_Subtype_stg,

                   cc_incident.RepWhereDisInd_stg as incd_RepWhereDisInd_stg,

                   cc_incident.PhantomVehicle_stg as incd_PhantomVehicle_stg,

                   cc_incident.LoadCommandID_stg as incd_LoadCommandID_stg,

                   cc_incident.DisabledDueToAccident_stg as incd_DisabledDueToAccident_stg,

                   cc_incident.PayoffAmount_alfa_stg as incd_PayoffAmount_alfa_stg,

                   cc_incident.FireBurnEngine_stg as incd_FireBurnEngine_stg,

                   cc_incident.ComponentsMissing_stg as incd_ComponentsMissing_stg,

                   cc_incident.DateVehicleSold_stg as incd_DateVehicleSold_stg,

                   cc_incident.VehTowedInd_stg as incd_VehTowedInd_stg,

                   cc_incident.CollisionPoint_stg as incd_CollisionPoint_stg,

                   cc_incident.MealsPeople_stg as incd_MealsPeople_stg,

                   cc_incident.ExtDamagetxt_stg as incd_ExtDamagetxt_stg,

                   cc_incident.NumSprinkler_stg as incd_NumSprinkler_stg,

                   cc_incident.SprinklerType_stg as incd_SprinklerType_stg,

                   cc_incident.AffdvCmplInd_stg as incd_AffdvCmplInd_stg,

                   cc_incident.LostWages_stg as incd_LostWages_stg,

                   cc_incident.Retired_stg as incd_Retired_stg,

                   cc_incident.DelayOnly_stg as incd_DelayOnly_stg,

                   cc_incident.PropertyID_stg as incd_PropertyID_stg,

                   cc_incident.InteriorMissing_stg as incd_InteriorMissing_stg,

                   cc_incident.DwellingSalvageStatus_alfa_stg as incd_DwellingSalvageStatus_alfa_stg,

                   cc_incident.VehicleSubmerged_stg as incd_VehicleSubmerged_stg,

                   cc_incident.TrafficViolation_stg as incd_TrafficViolation_stg,

                   cc_incident.CreateTime_stg as incd_CreateTime_stg,

                   cc_incident.DwellingSalvageProceeds_alfa_stg as incd_DwellingSalvageProceeds_alfa_stg,

                   cc_incident.ReturnToWorkActual_stg as incd_ReturnToWorkActual_stg,

                   cc_incident.VehicleDirection_stg as incd_VehicleDirection_stg,

                   cc_incident.AppraisalFirstAppointment_stg as incd_AppraisalFirstAppointment_stg,

                   cc_incident.VehicleOperable_stg as incd_VehicleOperable_stg,

                   cc_incident.RentalDailyRate_stg as incd_RentalDailyRate_stg,

                   cc_incident.LotNumber_stg as incd_LotNumber_stg,

                   cc_incident.WaterLevelDash_stg as incd_WaterLevelDash_stg,

                   cc_incident.VehStolenInd_stg as incd_VehStolenInd_stg,

                   cc_incident.EstRepairTime_stg as incd_EstRepairTime_stg,

                   cc_incident.MoldInvolved_stg as incd_MoldInvolved_stg,

                   cc_incident.DetailedInjuryType_stg as incd_DetailedInjuryType_stg,

                   cc_incident.DamagedAreaSize_stg as incd_DamagedAreaSize_stg,

                   cc_incident.NumSprinkOper_stg as incd_NumSprinkOper_stg,

                   cc_incident.PropertyDesc_stg as incd_PropertyDesc_stg,

                   cc_incident.VehicleACV_stg as incd_VehicleACV_stg,

                   cc_incident.OwnersPermission_stg as incd_OwnersPermission_stg,

                   cc_incident.MealsRate_stg as incd_MealsRate_stg,

                   cc_incident.DwellingSalvageExpenses_alfa_stg as incd_DwellingSalvageExpenses_alfa_stg,

                   cc_incident.ReturnToModWorkValid_stg as incd_ReturnToModWorkValid_stg,

                   cc_incident.VehCondType_stg as incd_VehCondType_stg,

/*                   cc_incident.VehicleLossParty_stg as incd_VehicleLossParty_stg,*/ /*EIM-37658*/

                   cc_incident.RoofMaterial_stg as incd_RoofMaterial_stg,

                   cc_incident.Description_stg as incd_Description_stg,

                   cc_incident.VehicleRollOver_stg as incd_VehicleRollOver,

                   cc_claim.ClaimNumber_stg as clm_ClaimNumber_stg,

                   cc_contact.Name_stg as ccon_Name_stg,

                   cc_contact.PublicID_stg as ccon_PublicID_stg,

                   CASE

                       WHEN cc_vehicle.PolicySystemId_stg IS NOT NULL THEN SUBSTR(cc_vehicle.PolicySystemId_stg, POSITION('':'',cc_vehicle.PolicySystemId_stg)+1)

                       WHEN (cc_vehicle.PolicySystemId_stg IS NULL

AND cc_vehicle.Vin_stg IS NOT NULL) THEN (''VIN:'' || cc_vehicle.vin_stg)/* CONCAT(''VIN:'',cc_vehicle.vin_stg) */


                       WHEN (cc_vehicle.PolicySystemId_stg IS NULL

                             AND cc_vehicle.Vin_stg IS NULL

AND cc_vehicle.LicensePlate_stg IS NOT NULL) THEN (''LP:'' || cc_vehicle.licenseplate_stg)/* concat(''LP:'',cc_vehicle.licenseplate_stg) */


                       WHEN (cc_vehicle.PolicySystemId_stg IS NULL

                             AND cc_vehicle.Vin_stg IS NULL

                             AND cc_vehicle.LicensePlate_stg IS NULL) THEN cc_vehicle.PublicID_stg

                   END NK_VEHICLE_stg,

                   cc_claimcontactrole.Role_stg clmcon_Role_stg,

cc_injurydiagnosis.Comments_stg as injd_Comments_stg, /* cc_icdcode.Code, */
cctl_icdbodysystem.typecode_stg as icd_typecode_stg,

                                                                      cc_incident.VehicleSalvageAbandoned_alfa_stg as incd_VehicleSalvageAbandoned_alfa_stg,

                                                                      cc_vehicle.PolicySystemId_stg AS vehicle_policysystemid_stg,

                                                                      cc_policylocation.PolicySystemId_stg AS policyloc_policysystemid_stg,

                                                                      cctl_constructiontype_alfa.TYPECODE_stg AS Construction_Type_Cd_stg,

                                                                      cc_injurydiagnosis.updatetime_stg AS updatetime_cc_injurydiagnosis_stg,

                                                                      (:start_dttm) AS start_dttm,

                                                                      (:end_dttm) AS end_dttm

   FROM DB_T_PROD_STAG.cc_incident

   INNER JOIN

     (SELECT cc_claim.*

      FROM DB_T_PROD_STAG.cc_claim

      INNER JOIN DB_T_PROD_STAG.cctl_claimstate ON cc_claim.State_stg= cctl_claimstate.id_stg

      WHERE cctl_claimstate.name_stg <> ''Draft'') cc_claim ON cc_claim.id_stg=cc_incident.ClaimID_stg

   LEFT OUTER JOIN DB_T_PROD_STAG.cc_vehicle ON cc_incident.VehicleID_stg=cc_vehicle.ID_stg

   LEFT OUTER JOIN DB_T_PROD_STAG.cc_injurydiagnosis ON cc_incident.ID_stg=cc_injurydiagnosis.InjuryIncidentID_stg

   LEFT OUTER JOIN DB_T_PROD_STAG.cc_icdcode ON cc_injurydiagnosis.ICDCode_stg=cc_icdcode.ID_stg

   LEFT OUTER JOIN DB_T_PROD_STAG.cctl_icdbodysystem ON cctl_icdbodysystem.ID_stg=cc_icdcode.BodySystem_stg

/*   LEFT OUTER JOIN DB_T_PROD_STAG.cctl_losspartytype ON cctl_losspartytype.ID_stg = cc_incident.VehicleLossParty_stg*/ /*EIM-37658*/

   LEFT OUTER JOIN DB_T_PROD_STAG.cc_address ON cc_claim.LossLocationID_stg = cc_address.ID_stg

   LEFT OUTER JOIN DB_T_PROD_STAG.cc_policylocation ON cc_policylocation.AddressID_stg= cc_address.ID_stg

   LEFT OUTER JOIN DB_T_PROD_STAG.cc_riskunit ON cc_riskunit.PolicyLocationID_stg = cc_policylocation.ID_stg

   LEFT OUTER JOIN DB_T_PROD_STAG.cctl_constructiontype_alfa ON cc_riskunit.ConstructionType_alfa_stg=cctl_constructiontype_alfa.ID_stg

   LEFT OUTER JOIN DB_T_PROD_STAG.cc_claimcontactrole ON cc_incident.ID_stg = cc_claimcontactrole.IncidentID_stg

   LEFT OUTER JOIN DB_T_PROD_STAG.cc_claimcontact ON cc_claimcontactrole.claimcontactid_stg=cc_claimcontact.id_stg

   LEFT OUTER JOIN DB_T_PROD_STAG.cc_contact ON cc_claimcontact.contactid_stg=cc_contact.id_stg

   WHERE cc_incident.updatetime_stg > (:start_dttm)

     AND cc_incident.updatetime_stg <= (:end_dttm) 

	 

) cc_incident

INNER JOIN

  (SELECT DISTINCT 	cc_vehicle.PublicID_stg as veh_PublicID_stg,

  					cc_vehicle.UpdateTime_stg as veh_UpdateTime_stg,

                   cc_vehicle.ID_stg as veh_ID_stg,

                   cc_vehicle.Vin_stg as veh_Vin_stg,

				    cc_vehicle.Model_stg as veh_Model_stg,

					cc_vehicle.Make_stg as veh_Make_stg,

					cc_vehicle.LicensePlate_stg as veh_LicensePlate_stg,

					cc_vehicle.PolicySystemId_stg as veh_PolicySystemId_stg,

                   cc_vehicle.Year_stg as veh_Year_stg,

				   cc_vehicle.Style_stg as veh_Style_stg,

                   (:start_dttm) AS start_dttm,

                   (:end_dttm) AS end_dttm

   FROM DB_T_PROD_STAG.cc_vehicle

   LEFT OUTER JOIN DB_T_PROD_STAG.cc_incident ON cc_vehicle.ID_stg =cc_incident.vehicleid_stg

LEFT OUTER JOIN /* DB_T_PROD_STAG.cc_riskunit on cc_vehicle.ID_stg = cc_riskunit.vehicleid join */
DB_T_PROD_STAG.cc_exposure ON cc_incident.id_stg =cc_exposure.incidentid_stg /* join

 DB_T_PROD_STAG.cctl_riskunit on

cc_riskunit.subtype = cctl_riskunit.id

where cctl_riskunit.TYPECODE = ''VehicleRU''

and */

   WHERE cc_vehicle.UpdateTime_stg > (:start_dttm)

     AND cc_vehicle.UpdateTime_stg <= (:end_dttm) 

	 

) cc_vehicle 

ON CAST (cc_incident.incd_VehicleID_stg AS integer) = cc_vehicle.veh_id_stg

LEFT OUTER JOIN DB_T_PROD_STAG.cctl_incident ON cc_incident.incd_Subtype_stg = cctl_incident.id_stg

LEFT OUTER JOIN DB_T_PROD_STAG.cctl_vehiclestyle ON cc_vehicle.veh_Style_stg = cctl_vehiclestyle.id_stg

WHERE veh_policysystemid_stg IS NULL)tmp 



qualify ROW_NUMBER() OVER(partition by id,type_code,classification_code order by Trans_Strt_Dttm desc) =1
) SRC
)
);


-- Component exp_all_source, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_all_source AS
(
SELECT
sq_pc_personalvehicle.Bodytype as Bodytype,
sq_pc_personalvehicle.Model as Model,
sq_pc_personalvehicle.Vin as MOTR_VEH_SER_NUM,
sq_pc_personalvehicle.Yearbuilt as MFG_YR_NUM,
sq_pc_personalvehicle.Manufacturer as Manufacturer,
sq_pc_personalvehicle.VehTypecode as MOTR_VEH_TYPE_CD,
ltrim ( rtrim ( TO_CHAR ( sq_pc_personalvehicle.FixedID ) ) ) as var_FIXEDID,
var_FIXEDID as out_ASSET_HOST_ID_VAL,
CASE WHEN LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE */ IS NULL THEN ''UNK'' ELSE LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE */ END as out_PRTY_ASSET_SBTYPE_CD,
CASE WHEN LKP_3.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN */ IS NULL THEN ''UNK'' ELSE LKP_4.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN */ END as OUT_CLASS_CD,
1 as out_CNTRL_ID,
:PRCS_ID as out_PRCS_ID,
LKP_5.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SRC_CD */ as o_SYS_SRC_CD,
CASE WHEN sq_pc_personalvehicle.Createtime IS NULL THEN to_date ( ''1900-01-01'' , ''yyyy-mm-dd'' ) ELSE sq_pc_personalvehicle.Createtime END as o_Createtime,
sq_pc_personalvehicle.End_dt as End_dt,
sq_pc_personalvehicle.veh_mf_name as veh_mf_name,
sq_pc_personalvehicle.mf_name_frdm_ind as mf_name_frdm_ind,
sq_pc_personalvehicle.make_name_frdm_ind as make_name_frdm_ind,
sq_pc_personalvehicle.Trans_strt_dttm as Trans_strt_dttm,
sq_pc_personalvehicle.rnk as rnk,
sq_pc_personalvehicle.Name as Name,
sq_pc_personalvehicle.HorsePower_alfa as HorsePower_alfa,
sq_pc_personalvehicle.source_record_id,
row_number() over (partition by sq_pc_personalvehicle.source_record_id order by sq_pc_personalvehicle.source_record_id) as RNK1
FROM
sq_pc_personalvehicle
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = sq_pc_personalvehicle.sb_type_cd
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = sq_pc_personalvehicle.sb_type_cd
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN LKP_3 ON LKP_3.SRC_IDNTFTN_VAL = sq_pc_personalvehicle.CLASS_CD
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN LKP_4 ON LKP_4.SRC_IDNTFTN_VAL = sq_pc_personalvehicle.CLASS_CD
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_SRC_CD LKP_5 ON LKP_5.SRC_IDNTFTN_VAL = sq_pc_personalvehicle.SYS_SRC_CD
QUALIFY RNK1 = 1
);


-- Component LKP_PRTY_ASSET_ID, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_PRTY_ASSET_ID AS
(
SELECT
LKP.PRTY_ASSET_ID,
exp_all_source.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_all_source.source_record_id ORDER BY LKP.PRTY_ASSET_ID asc,LKP.ASSET_HOST_ID_VAL asc,LKP.PRTY_ASSET_SBTYPE_CD asc,LKP.PRTY_ASSET_CLASFCN_CD asc,LKP.ASSET_INSRNC_HIST_TYPE_CD asc,LKP.ASSET_DESC asc,LKP.PRTY_ASSET_NAME asc,LKP.PRTY_ASSET_STRT_DTTM asc,LKP.PRTY_ASSET_END_DTTM asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc,LKP.SRC_SYS_CD asc) RNK
FROM
exp_all_source
LEFT JOIN (
SELECT PRTY_ASSET.PRTY_ASSET_ID as PRTY_ASSET_ID, PRTY_ASSET.ASSET_INSRNC_HIST_TYPE_CD as ASSET_INSRNC_HIST_TYPE_CD, PRTY_ASSET.ASSET_DESC as ASSET_DESC, PRTY_ASSET.PRTY_ASSET_NAME as PRTY_ASSET_NAME, PRTY_ASSET.PRTY_ASSET_STRT_DTTM as PRTY_ASSET_STRT_DTTM, PRTY_ASSET.PRTY_ASSET_END_DTTM as PRTY_ASSET_END_DTTM, PRTY_ASSET.EDW_STRT_DTTM as EDW_STRT_DTTM, PRTY_ASSET.EDW_END_DTTM as EDW_END_DTTM, PRTY_ASSET.SRC_SYS_CD as SRC_SYS_CD, PRTY_ASSET.ASSET_HOST_ID_VAL as ASSET_HOST_ID_VAL, PRTY_ASSET.PRTY_ASSET_SBTYPE_CD as PRTY_ASSET_SBTYPE_CD, PRTY_ASSET.PRTY_ASSET_CLASFCN_CD as PRTY_ASSET_CLASFCN_CD 
FROM DB_T_PROD_CORE.PRTY_ASSET 
QUALIFY ROW_NUMBER() OVER(PARTITION BY  ASSET_HOST_ID_VAL,PRTY_ASSET_SBTYPE_CD,PRTY_ASSET_CLASFCN_CD ORDER BY EDW_END_DTTM DESC) = 1
) LKP ON LKP.ASSET_HOST_ID_VAL = exp_all_source.out_ASSET_HOST_ID_VAL AND LKP.PRTY_ASSET_SBTYPE_CD = exp_all_source.out_PRTY_ASSET_SBTYPE_CD AND LKP.PRTY_ASSET_CLASFCN_CD = exp_all_source.OUT_CLASS_CD
QUALIFY RNK = 1
);


-- Component exp_data_transformation, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_data_transformation AS
(
SELECT
CASE WHEN exp_all_source.MOTR_VEH_SER_NUM IS NULL THEN ''UNK'' ELSE UPPER ( exp_all_source.MOTR_VEH_SER_NUM ) END as o_MOTR_VEH_SER_NUM,
CASE WHEN exp_all_source.MFG_YR_NUM IS NULL THEN ''UNK'' ELSE UPPER ( exp_all_source.MFG_YR_NUM ) END as o_MGF_YR_NUM,
exp_all_source.MOTR_VEH_TYPE_CD as MOTR_VEH_TYPE_CD,
LKP_PRTY_ASSET_ID.PRTY_ASSET_ID as PRTY_ASSET_ID,
CASE WHEN exp_all_source.Model IS NULL THEN ''UNK'' ELSE UPPER ( exp_all_source.Model ) END as o_MODL_NM,
exp_all_source.out_PRCS_ID as PRCS_ID,
exp_all_source.Bodytype as v_out_bodytype,
CASE WHEN LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT */ IS NULL THEN UPPER ( :P_DEFAULT_STR_CD ) ELSE UPPER ( LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT */ ) END as v_motr_veh_type_cd,
CASE WHEN LKP_3.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT3 */ IS NULL THEN UPPER ( :P_DEFAULT_STR_CD ) ELSE UPPER ( LKP_4.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT3 */ ) END as v_veh_mhgr_cd,
CASE WHEN LKP_5.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_XMISN_TYPE */ IS NULL THEN UPPER ( :P_DEFAULT_STR_CD ) ELSE UPPER ( LKP_6.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_XMISN_TYPE */ ) END as XMISN_TYPE_CD,
UPPER ( :P_DEFAULT_STR_CD ) as MODFN_CD,
CASE WHEN v_motr_veh_type_cd IS NULL THEN ''UNK'' ELSE v_motr_veh_type_cd END as out_MOTR_VEH_TYPE_CD,
CASE WHEN LKP_7.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_BODY_TYPE */ IS NULL THEN ''UNK'' ELSE LKP_8.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_BODY_TYPE */ END as out_BODY_TYPE,
CURRENT_TIMESTAMP as EDW_STRT_DTTM,
TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as EDW_END_DTTM,
exp_all_source.o_Createtime as o_Createtime,
exp_all_source.End_dt as End_dt,
exp_all_source.veh_mf_name as veh_mf_name,
exp_all_source.mf_name_frdm_ind as mf_name_frdm_ind,
exp_all_source.make_name_frdm_ind as make_name_frdm_ind,
exp_all_source.Trans_strt_dttm as Trans_strt_dttm,
exp_all_source.rnk as rnk,
exp_all_source.HorsePower_alfa as HorsePower_alfa,
exp_all_source.source_record_id,
row_number() over (partition by exp_all_source.source_record_id order by exp_all_source.source_record_id) as RNK2
FROM
exp_all_source
INNER JOIN LKP_PRTY_ASSET_ID ON exp_all_source.source_record_id = LKP_PRTY_ASSET_ID.source_record_id
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = exp_all_source.MOTR_VEH_TYPE_CD
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = exp_all_source.MOTR_VEH_TYPE_CD
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT3 LKP_3 ON LKP_3.SRC_IDNTFTN_VAL = exp_all_source.Manufacturer
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT3 LKP_4 ON LKP_4.SRC_IDNTFTN_VAL = exp_all_source.Manufacturer
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_XMISN_TYPE LKP_5 ON LKP_5.SRC_IDNTFTN_VAL = exp_all_source.Name
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_XMISN_TYPE LKP_6 ON LKP_6.SRC_IDNTFTN_VAL = exp_all_source.Name
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_BODY_TYPE LKP_7 ON LKP_7.SRC_IDNTFTN_VAL = exp_all_source.Bodytype
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_BODY_TYPE LKP_8 ON LKP_8.SRC_IDNTFTN_VAL = exp_all_source.Bodytype
QUALIFY RNK2 = 1
);


-- Component LKP_MOTR_VEH, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_MOTR_VEH AS
(
SELECT
LKP.PRTY_ASSET_ID,
LKP.MOTR_VEH_TYPE_CD,
LKP.BODY_TYPE_CD,
LKP.XMISN_TYPE_CD,
LKP.MODFN_CD,
LKP.MFG_YR_NUM,
LKP.MODL_NAME,
LKP.MOTR_VEH_SER_NUM,
LKP.ENGN_PWR_MEAS,
LKP.TRANS_STRT_DTTM,
LKP.VEH_MFGR_NAME,
LKP.VEH_MFGR_NAME_FREFM_IND,
LKP.MODL_NAME_FREFM_IND,
exp_data_transformation.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_data_transformation.source_record_id ORDER BY LKP.PRTY_ASSET_ID asc,LKP.MOTR_VEH_TYPE_CD asc,LKP.BODY_TYPE_CD asc,LKP.XMISN_TYPE_CD asc,LKP.MODFN_CD asc,LKP.MFG_YR_NUM asc,LKP.MODL_NAME asc,LKP.MOTR_VEH_SER_NUM asc,LKP.ENGN_PWR_MEAS asc,LKP.TRANS_STRT_DTTM asc,LKP.VEH_MFGR_NAME asc,LKP.VEH_MFGR_NAME_FREFM_IND asc,LKP.MODL_NAME_FREFM_IND asc) RNK
FROM
exp_data_transformation
LEFT JOIN (
SELECT MOTR_VEH.MOTR_VEH_TYPE_CD as MOTR_VEH_TYPE_CD, MOTR_VEH.BODY_TYPE_CD as BODY_TYPE_CD, MOTR_VEH.XMISN_TYPE_CD as XMISN_TYPE_CD, MOTR_VEH.MODFN_CD as MODFN_CD, MOTR_VEH.MFG_YR_NUM as MFG_YR_NUM, MOTR_VEH.MODL_NAME as MODL_NAME, MOTR_VEH.MOTR_VEH_SER_NUM as MOTR_VEH_SER_NUM, MOTR_VEH.ENGN_PWR_MEAS as ENGN_PWR_MEAS, MOTR_VEH.TRANS_STRT_DTTM as TRANS_STRT_DTTM, MOTR_VEH.VEH_MFGR_NAME as VEH_MFGR_NAME, MOTR_VEH.VEH_MFGR_NAME_FREFM_IND as VEH_MFGR_NAME_FREFM_IND, MOTR_VEH.MODL_NAME_FREFM_IND as MODL_NAME_FREFM_IND, MOTR_VEH.PRTY_ASSET_ID as PRTY_ASSET_ID FROM DB_T_PROD_CORE.MOTR_VEH QUALIFY ROW_NUMBER() OVER(PARTITION BY PRTY_ASSET_ID  ORDER BY EDW_END_DTTM desc) = 1
) LKP ON LKP.PRTY_ASSET_ID = exp_data_transformation.PRTY_ASSET_ID
QUALIFY RNK = 1
);


-- Component exp_compare_data, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_compare_data AS
(
SELECT
LKP_MOTR_VEH.PRTY_ASSET_ID as lkp_PRTY_ASSET_ID,
MD5 ( LTRIM ( RTRIM ( LKP_MOTR_VEH.MOTR_VEH_TYPE_CD ) ) || LTRIM ( RTRIM ( LKP_MOTR_VEH.BODY_TYPE_CD ) ) || LTRIM ( RTRIM ( LKP_MOTR_VEH.XMISN_TYPE_CD ) ) || LTRIM ( RTRIM ( LKP_MOTR_VEH.MODFN_CD ) ) || LTRIM ( RTRIM ( LKP_MOTR_VEH.MFG_YR_NUM ) ) || LTRIM ( RTRIM ( LKP_MOTR_VEH.MODL_NAME ) ) || LTRIM ( RTRIM ( LKP_MOTR_VEH.MOTR_VEH_SER_NUM ) ) || LTRIM ( RTRIM ( LKP_MOTR_VEH.VEH_MFGR_NAME ) ) || LTRIM ( RTRIM ( LKP_MOTR_VEH.VEH_MFGR_NAME_FREFM_IND ) ) || LTRIM ( RTRIM ( LKP_MOTR_VEH.MODL_NAME_FREFM_IND ) ) || to_char ( ltrim ( rtrim ( LKP_MOTR_VEH.ENGN_PWR_MEAS ) ) ) ) as v_lkp_checksum,
exp_data_transformation.PRTY_ASSET_ID as in_PRTY_ASSET_ID,
exp_data_transformation.out_MOTR_VEH_TYPE_CD as in_MOTR_VEH_TYPE_CD,
exp_data_transformation.out_BODY_TYPE as in_BODY_TYPE,
exp_data_transformation.XMISN_TYPE_CD as in_XMISN_TYPE_CD,
exp_data_transformation.MODFN_CD as in_MODFN_CD,
exp_data_transformation.o_MGF_YR_NUM as in_MFG_YR_NUM,
exp_data_transformation.o_MODL_NM as in_MODL_NM,
exp_data_transformation.o_MOTR_VEH_SER_NUM as in_MOTR_VEH_SER_NUM,
exp_data_transformation.veh_mf_name as veh_mf_name,
exp_data_transformation.mf_name_frdm_ind as mf_name_frdm_ind,
exp_data_transformation.make_name_frdm_ind as make_name_frdm_ind,
exp_data_transformation.HorsePower_alfa as HorsePower_alfa,
MD5 ( LTRIM ( RTRIM ( exp_data_transformation.out_MOTR_VEH_TYPE_CD ) ) || LTRIM ( RTRIM ( exp_data_transformation.out_BODY_TYPE ) ) || LTRIM ( RTRIM ( exp_data_transformation.XMISN_TYPE_CD ) ) || LTRIM ( RTRIM ( exp_data_transformation.MODFN_CD ) ) || LTRIM ( RTRIM ( exp_data_transformation.o_MGF_YR_NUM ) ) || LTRIM ( RTRIM ( exp_data_transformation.o_MODL_NM ) ) || LTRIM ( RTRIM ( exp_data_transformation.o_MOTR_VEH_SER_NUM ) ) || LTRIM ( RTRIM ( exp_data_transformation.veh_mf_name ) ) || LTRIM ( RTRIM ( exp_data_transformation.mf_name_frdm_ind ) ) || LTRIM ( RTRIM ( exp_data_transformation.make_name_frdm_ind ) ) || to_char ( LTRIM ( RTRIM ( exp_data_transformation.HorsePower_alfa ) ) ) ) as v_in_checksum,
exp_data_transformation.PRCS_ID as PRCS_ID,
exp_data_transformation.EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_data_transformation.Trans_strt_dttm as Trans_strt_dttm,
exp_data_transformation.EDW_END_DTTM as in_EDW_END_DTTM,
CASE WHEN v_lkp_checksum IS NULL THEN ''I'' ELSE CASE WHEN v_lkp_checksum != v_in_checksum THEN ''U'' ELSE ''R'' END END as calc_ins_upd,
CASE WHEN exp_data_transformation.Trans_strt_dttm IS NULL THEN to_date ( ''01/01/1900'' , ''mm/dd/yyyy'' ) ELSE exp_data_transformation.Trans_strt_dttm END as o_Trans_strt_dttm1,
exp_data_transformation.rnk as rnk,
LKP_MOTR_VEH.TRANS_STRT_DTTM as LKP_TRANS_STRT_DTTM1,
exp_data_transformation.source_record_id
FROM
exp_data_transformation
INNER JOIN LKP_MOTR_VEH ON exp_data_transformation.source_record_id = LKP_MOTR_VEH.source_record_id
);


-- Component rtr_mtr_veh_INSERT, Type ROUTER Output Group INSERT
CREATE OR REPLACE TEMPORARY TABLE rtr_mtr_veh_INSERT AS
(SELECT
exp_compare_data.lkp_PRTY_ASSET_ID as lkp_PRTY_ASSET_ID,
exp_compare_data.in_PRTY_ASSET_ID as in_PRTY_ASSET_ID,
exp_compare_data.in_MOTR_VEH_TYPE_CD as in_MOTR_VEH_TYPE_CD,
exp_compare_data.in_BODY_TYPE as in_BODY_TYPE,
exp_compare_data.in_XMISN_TYPE_CD as in_XMISN_TYPE_CD,
exp_compare_data.in_MODFN_CD as in_MODFN_CD,
exp_compare_data.in_MFG_YR_NUM as in_MFG_YR_NUM,
exp_compare_data.in_MODL_NM as in_MODL_NM,
exp_compare_data.in_MOTR_VEH_SER_NUM as in_MOTR_VEH_SER_NUM,
exp_compare_data.PRCS_ID as PRCS_ID,
exp_compare_data.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_compare_data.in_EDW_END_DTTM as in_EDW_END_DTTM,
exp_compare_data.calc_ins_upd as calc_ins_upd,
exp_compare_data.veh_mf_name as veh_mf_name,
exp_compare_data.mf_name_frdm_ind as mf_name_frdm_ind,
exp_compare_data.make_name_frdm_ind as make_name_frdm_ind,
exp_compare_data.o_Trans_strt_dttm1 as Trans_strt_dttm,
exp_compare_data.rnk as rnk,
exp_compare_data.LKP_TRANS_STRT_DTTM1 as LKP_TRANS_STRT_DTTM1,
exp_compare_data.HorsePower_alfa as HorsePower_alfa,
exp_compare_data.source_record_id
FROM
exp_compare_data
WHERE CASE WHEN exp_compare_data.calc_ins_upd = ''I'' THEN TRUE ELSE CASE WHEN ( exp_compare_data.calc_ins_upd = ''U'' AND exp_compare_data.o_Trans_strt_dttm1 > exp_compare_data.LKP_TRANS_STRT_DTTM1 ) THEN TRUE ELSE $3 END END);


-- Component upd_motr_veh_insert, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_motr_veh_insert AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_mtr_veh_INSERT.in_PRTY_ASSET_ID as in_PRTY_ASSET_ID1,
rtr_mtr_veh_INSERT.in_MOTR_VEH_TYPE_CD as in_MOTR_VEH_TYPE_CD1,
rtr_mtr_veh_INSERT.in_BODY_TYPE as in_BODY_TYPE1,
rtr_mtr_veh_INSERT.in_XMISN_TYPE_CD as in_XMISN_TYPE_CD1,
rtr_mtr_veh_INSERT.in_MODFN_CD as in_MODFN_CD1,
rtr_mtr_veh_INSERT.in_MFG_YR_NUM as in_MFG_YR_NUM1,
rtr_mtr_veh_INSERT.in_MODL_NM as in_MODL_NM1,
rtr_mtr_veh_INSERT.in_MOTR_VEH_SER_NUM as in_MOTR_VEH_SER_NUM1,
rtr_mtr_veh_INSERT.PRCS_ID as PRCS_ID1,
rtr_mtr_veh_INSERT.in_EDW_END_DTTM as in_EDW_END_DTTM1,
rtr_mtr_veh_INSERT.veh_mf_name as veh_mf_name1,
rtr_mtr_veh_INSERT.mf_name_frdm_ind as mf_name_frdm_ind1,
rtr_mtr_veh_INSERT.make_name_frdm_ind as make_name_frdm_ind1,
rtr_mtr_veh_INSERT.Trans_strt_dttm as Trans_strt_dttm1,
rtr_mtr_veh_INSERT.rnk as rnk1,
rtr_mtr_veh_INSERT.HorsePower_alfa as HorsePower_alfa1,
0 as UPDATE_STRATEGY_ACTION,
rtr_mtr_veh_INSERT.SOURCE_RECORD_ID
FROM
rtr_mtr_veh_INSERT
);


-- Component exp_pass_to_target_insert, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_target_insert AS
(
SELECT
upd_motr_veh_insert.in_PRTY_ASSET_ID1 as in_PRTY_ASSET_ID1,
upd_motr_veh_insert.in_MOTR_VEH_TYPE_CD1 as in_MOTR_VEH_TYPE_CD1,
upd_motr_veh_insert.in_BODY_TYPE1 as in_BODY_TYPE1,
upd_motr_veh_insert.in_XMISN_TYPE_CD1 as in_XMISN_TYPE_CD1,
upd_motr_veh_insert.in_MODFN_CD1 as in_MODFN_CD1,
upd_motr_veh_insert.in_MFG_YR_NUM1 as in_MFG_YR_NUM1,
upd_motr_veh_insert.in_MODL_NM1 as in_MODL_NM1,
upd_motr_veh_insert.in_MOTR_VEH_SER_NUM1 as in_MOTR_VEH_SER_NUM1,
upd_motr_veh_insert.PRCS_ID1 as PRCS_ID1,
upd_motr_veh_insert.in_EDW_END_DTTM1 as in_EDW_END_DTTM1,
upd_motr_veh_insert.veh_mf_name1 as veh_mf_name1,
upd_motr_veh_insert.mf_name_frdm_ind1 as mf_name_frdm_ind1,
upd_motr_veh_insert.make_name_frdm_ind1 as make_name_frdm_ind1,
upd_motr_veh_insert.Trans_strt_dttm1 as Trans_strt_dttm1,
DATEADD (
  SECOND,
  (2 * (upd_motr_veh_insert.rnk1 - 1)),
  CURRENT_TIMESTAMP()
) as in_EDW_STRT_DTTM1,
IFNULL(TRY_TO_DECIMAL(upd_motr_veh_insert.HorsePower_alfa1), 0) as ENGN_PWR_MEAS,
upd_motr_veh_insert.source_record_id
FROM
upd_motr_veh_insert
);


-- Component tgt_motr_veh_insert, Type TARGET 
INSERT INTO DB_T_PROD_CORE.MOTR_VEH
(
PRTY_ASSET_ID,
MOTR_VEH_TYPE_CD,
VEH_MFGR_NAME,
VEH_MFGR_NAME_FREFM_IND,
BODY_TYPE_CD,
XMISN_TYPE_CD,
MODFN_CD,
MFG_YR_NUM,
MODL_NAME,
MODL_NAME_FREFM_IND,
MOTR_VEH_SER_NUM,
ENGN_PWR_MEAS,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM
)
SELECT
exp_pass_to_target_insert.in_PRTY_ASSET_ID1 as PRTY_ASSET_ID,
exp_pass_to_target_insert.in_MOTR_VEH_TYPE_CD1 as MOTR_VEH_TYPE_CD,
exp_pass_to_target_insert.veh_mf_name1 as VEH_MFGR_NAME,
exp_pass_to_target_insert.mf_name_frdm_ind1 as VEH_MFGR_NAME_FREFM_IND,
exp_pass_to_target_insert.in_BODY_TYPE1 as BODY_TYPE_CD,
exp_pass_to_target_insert.in_XMISN_TYPE_CD1 as XMISN_TYPE_CD,
exp_pass_to_target_insert.in_MODFN_CD1 as MODFN_CD,
exp_pass_to_target_insert.in_MFG_YR_NUM1 as MFG_YR_NUM,
exp_pass_to_target_insert.in_MODL_NM1 as MODL_NAME,
exp_pass_to_target_insert.make_name_frdm_ind1 as MODL_NAME_FREFM_IND,
exp_pass_to_target_insert.in_MOTR_VEH_SER_NUM1 as MOTR_VEH_SER_NUM,
exp_pass_to_target_insert.ENGN_PWR_MEAS as ENGN_PWR_MEAS,
exp_pass_to_target_insert.PRCS_ID1 as PRCS_ID,
exp_pass_to_target_insert.in_EDW_STRT_DTTM1 as EDW_STRT_DTTM,
exp_pass_to_target_insert.in_EDW_END_DTTM1 as EDW_END_DTTM,
exp_pass_to_target_insert.Trans_strt_dttm1 as TRANS_STRT_DTTM
FROM
exp_pass_to_target_insert;


-- Component tgt_motr_veh_insert, Type Post SQL 
UPDATE DB_T_PROD_CORE.MOTR_VEH FROM  

(

SELECT	distinct PRTY_ASSET_ID,EDW_STRT_DTTM,

max(EDW_STRT_DTTM) over (partition by PRTY_ASSET_ID ORDER BY EDW_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND'' 

 as lead1, 

max(TRANS_STRT_DTTM) over (partition by PRTY_ASSET_ID ORDER BY EDW_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND'' 

 as lead

FROM DB_T_PROD_CORE.MOTR_VEH 

)  A

set TRANS_END_DTTM=  A.lead, 

EDW_END_DTTM=A.lead1

where  MOTR_VEH.EDW_STRT_DTTM = A.EDW_STRT_DTTM

and MOTR_VEH.PRTY_ASSET_ID=A.PRTY_ASSET_ID

and MOTR_VEH.TRANS_STRT_DTTM <>MOTR_VEH.TRANS_END_DTTM

and lead is not null;


END; 
';