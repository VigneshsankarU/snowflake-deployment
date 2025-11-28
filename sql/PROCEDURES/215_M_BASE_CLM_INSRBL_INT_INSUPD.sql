-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_CLM_INSRBL_INT_INSUPD("RUN_ID" VARCHAR)
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

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM in ( ''derived'' ,''pcx_holineschcovitemcov_alfa.ChoiceTerm1'', ''cctl_contentlineitemschedule'')

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


-- Component LKP_TERADATA_ETL_REF_XLAT_CLM_INSRBL_INT_TYPE, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_CLM_INSRBL_INT_TYPE AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''CLM_INSRBL_INT_TYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''cctl_incident.typecode'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''GW'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component SQ_cc_incident, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_cc_incident AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as ClaimNumber,
$2 as TYPECODE2,
$3 as Description,
$4 as VehicleOperable,
$5 as LossEstimate,
$6 as OwnerRetainingSalvage,
$7 as OwnersPermission,
$8 as Name,
$9 as SubroPotential_alfa,
$10 as OdomRead,
$11 as TYPECODE,
$12 as TYPECODE1,
$13 as ID,
$14 as TotalLoss,
$15 as insurable_key,
$16 as PRTY_ASSET_SB_TYPE,
$17 as InsurableInterestCategory,
$18 as clasfcn_cd,
$19 as medicare_indicator,
$20 as VehicleSalvageAbandoned_alfa,
$21 as Speed,
$22 as TYPECODE_personrelationtype,
$23 as CLM_SRC_CD,
$24 as CreateTime,
$25 as UpdateTime,
$26 as CLM_INSRBL_INT_TYPE,
$27 as Retired,
$28 as DateSalvageAssigned,
$29 as VehStolenInd,
$30 as VehRecoveredInd_alfa,
$31 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select	distinct 

		cc_claim.ClaimNumber_stg as ClaimNumber,

		cast('''' as varchar(50)) as typecode,

		cast('''' as varchar(1333)) as description,

			

		cast('''' as varchar(50)) as VehicleOperable,

		cast('''' as decimal(18,2)) as lossestimate,

		cast('''' as varchar(50)) as ownerretainingsalvage,

		 cast('''' as varchar(50)) as OwnersPermission,

		 cc_contact.name_stg as name,

		 cast('''' as varchar(50)) as SubroPotential_alfa,

		 cast(null as INTEGER)as  OdomRead,

		 cast('''' as varchar(50)) as TYPECODE,

		 cast('''' as varchar(50)) as TYPECODE,

		cast(null as BIGINT) as ID,

		cast('''' as varchar(50)) as totalloss,

		cast(cc_contact.publicid_stg as varchar(64)) as insurable_key,

		cast('''' as varchar(50)) AS PRTY_ASSET_SB_TYPE,

		cast(''PERSON'' AS varchar(50))as INSUREKEY,

		cast('''' as varchar(50)) as clasfcn_cd,

		cctl_yesno.typecode_stg as medicare_indicator,

		cast('''' as varchar(50)) as VehicleSalvageAbandoned_alfa,

		 cast(null as INTEGER) as speed,

			cast('''' as varchar(50)) as TYPECODE,

		cast(''SRC_SYS6'' as varchar(50)) as CLM_SRC_CD,

		TO_TIMESTAMP(''1900-01-01 00:00:00'', ''yyyy-mm-dd hh24:mi:ss'') createtime1,

		cc_claimcontactrole.updatetime_stg as updatetime,

		cast(''InjuryIncident'' as varchar(50)) as CLM_INSRBL_INT_TYPE,

		cc_claimcontactrole.Retired_stg as Retired,

		CAST(NULL AS TIMESTAMP) DateSalvageAssigned,

		cast('''' as varchar(50)) as VehStolenInd,

		cast('''' as varchar(50)) as VehRecoveredInd_alfa 

		from	

			DB_T_PROD_STAG.cc_claimcontactrole

		left outer join DB_T_PROD_STAG.cc_Incident 

			on cc_incident.id_stg = cc_claimcontactrole.IncidentID_stg

		left outer join DB_T_PROD_STAG.cctl_yesno 

			on cc_incident.medicare_alfa_stg = cctl_yesno.id_stg

		left outer join DB_T_PROD_STAG.cctl_incident 

			on cc_incident.Subtype_stg = cctl_incident.id_stg

		left outer join DB_T_PROD_STAG.cc_claimcontact 

			on cc_claimcontactrole.claimcontactid_stg=cc_claimcontact.id_stg

		inner join (

			select	cc_claim.ClaimNumber_stg, cc_claim.State_stg, cc_claim.id_stg

			from	DB_T_PROD_STAG.cc_claim 

			inner join DB_T_PROD_STAG.cctl_claimstate 

				on cc_claim.State_stg= cctl_claimstate.id_stg 

			where	cctl_claimstate.name_stg <> ''Draft'') cc_claim 

			on cc_claim.id_stg = cc_claimcontact.ClaimID_stg

		inner join DB_T_PROD_STAG.cc_contact 

			on cc_claimcontact.contactid_stg=cc_contact.id_stg

		where	cctl_incident.TYPECODE_stg=''InjuryIncident''

			and cc_claimcontactrole.UpdateTime_stg > (:START_DTTM) 

			AND cc_claimcontactrole.UpdateTime_stg <= (:END_DTTM)

		AND	 cctl_incident.TYPECODE_stg = ''InjuryIncident'' 

			and cc_claim.ClaimNumber_stg is not null 

			and insurable_key is not null 

		QUALIFY	ROW_NUMBER() OVER (

		PARTITION BY cc_claim.ClaimNumber_stg,

			cc_contact.publicid_stg 

		ORDER BY cc_claimcontactrole.createtime_stg desc,

			cc_claimcontactrole.updatetime_stg DESC,

				cc_claimcontactrole.IncidentID_stg DESC)=1

		union

		Select	distinct 

		claimnumber,

		cctl_yesno.typecode_stg,

		cc_incident.description, 

		cast(VehicleOperable as varchar(50))as VehicleOperable,

		VehicleACV as lossestimate,

		ownerretainingsalvage, 

		OwnersPermission,

		cast('''' as varchar(50)) as name,

		SubroPotential_alfa,

		OdomRead,

		cctl_occupancytype.TYPECODE_stg,

		cctl_severitytype.TYPECODE_stg,

		cc_incident.ID,

		TotalLoss,

		cast(cc_incident.nk_vehicle as varchar(64))as  insurable_key,

		cast(''PRTY_ASSET_SBTYPE4'' AS  varchar(50)) as PRTY_ASSET_SB_TYPE,

			

		CAST(''ASSET'' AS varchar(50))as INSUREKEY,

		CAST(''PRTY_ASSET_CLASFCN3''  AS varchar(50))as clasfcn_cd,

		cast('''' as varchar(50)) as medicare_indicator,

		cast(cc_incident.VehicleSalvageAbandoned_alfa as varchar(50))as VehicleSalvageAbandoned_alfa,

		speed,

		cctl_personrelationtype.TYPECODE_stg,

		cast(''SRC_SYS6'' as varchar(50))as CLM_SRC_CD,

		cc_incident.createtime,

		cc_incident.updatetime,

		cast(''VehicleIncident'' as varchar(50)) as CLM_INSRBL_INT_TYPE,

		cc_incident.Retired as Retired,

		DateSalvageAssigned,

		VehStolenInd,

		VehRecoveredInd_alfa 

		from	(

		SELECT	DISTINCT CAST(

				CASE 

					When cc_incident.VehRecoveredInd_alfa_stg =1 then ''T'' 

					when cc_incident.VehRecoveredInd_alfa_stg= 0 then ''F'' 

				end  as varchar(50)) as VehRecoveredInd_alfa, cc_incident.Speed_stg as Speed,

				CAST(

				case 

					when cc_incident.TotalLoss_stg is null then 0 

					else cc_incident.TotalLoss_stg 

				end  as varchar(50))as TotalLoss, cc_incident.OdomRead_stg as OdomRead,
cast(cc_incident.OwnerRetainingSalvage_stg as varchar(50))as OwnerRetainingSalvage,

				cc_incident.DateSalvageAssigned_stg as DateSalvageAssigned, cc_incident.UpdateTime_stg as UpdateTime,

				cc_incident.ID_stg as ID, cast(

				CASE 

					when cc_incident.SubroPotential_alfa_stg = 1 then ''T'' 

					when cc_incident.SubroPotential_alfa_stg = 0 then ''F'' 

				end  as varchar(50))as SubroPotential_alfa, cc_incident.Retired_stg as Retired,

				cc_incident.CreateTime_stg as CreateTime, 

				CASE 

					When cc_incident.VehicleOperable_stg = 0 then ''F'' 

					When cc_incident.VehicleOperable_stg = 1 then ''T'' 

				end as  VehicleOperable, CAST(cc_incident.VehStolenInd_stg as varchar(50)) as VehStolenInd,

				cc_incident.VehicleACV_stg as VehicleACV, cast(cc_incident.OwnersPermission_stg as varchar(50)) as OwnersPermission,

				cc_incident.Description_stg as Description, cc_claim.ClaimNumber_stg as ClaimNumber,

				

		case 

					when cc_vehicle.PolicySystemId_stg is not null then SUBSTR(cc_vehicle.policysystemid_stg,

				POSITION('':'' IN cc_vehicle.policysystemid_stg)+1,LENGTH(cc_vehicle.policysystemid_stg))

					when (cc_vehicle.PolicySystemId_stg is null 

			and cc_vehicle.Vin_stg is not null) then  ''VIN:''||cc_vehicle.vin_stg 

					when (cc_vehicle.PolicySystemId_stg is null 

			and cc_vehicle.Vin_stg is null 

			and cc_vehicle.LicensePlate_stg is not null) then ''LP:''||cc_vehicle.licenseplate_stg

					when (cc_vehicle.PolicySystemId_stg is null 

			and cc_vehicle.Vin_stg is null 

			and cc_vehicle.LicensePlate_stg is null) then cc_vehicle.PublicID_stg

		end NK_VEHICLE,

				CASE 

					When cc_incident.VehicleSalvageAbandoned_alfa_stg = 0 then ''F'' 

					When cc_incident.VehicleSalvageAbandoned_alfa_stg = 1 then ''T'' 

				end as VehicleSalvageAbandoned_alfa, cc_Incident.DriverRelation_stg,

				cc_incident.severity_stg, cc_incident.attorneyrepresented_alfa_stg,

				cc_incident.OccupancyType_stg

		FROM

		DB_T_PROD_STAG.cc_Incident 

		inner join (

			select	cc_claim.ClaimNumber_stg, cc_claim.State_stg, cc_claim.id_stg

			from	DB_T_PROD_STAG.cc_claim 

			inner join DB_T_PROD_STAG.cctl_claimstate 

				on cc_claim.State_stg= cctl_claimstate.id_stg 

			where	cctl_claimstate.name_stg <> ''Draft'') cc_claim 

			on cc_claim.id_stg=cc_incident.claimid_stg

		left outer join DB_T_PROD_STAG.cc_vehicle 

			on cc_incident.vehicleid_stg=cc_vehicle.id_stg

		WHERE	

		cc_incident.UpdateTime_stg > :START_DTTM

			and cc_incident.UpdateTime_stg <= :END_DTTM) cc_Incident

		left outer join DB_T_PROD_STAG.cctl_personrelationtype 

			on cc_Incident.DriverRelation_stg=cctl_personrelationtype.ID_stg

		left outer join DB_T_PROD_STAG.cctl_severitytype 

			on cc_incident.severity_stg = cctl_severitytype.id_stg

		left outer join DB_T_PROD_STAG.cctl_yesno 

			on cc_incident.attorneyrepresented_alfa_stg=cctl_yesno.id_stg

		left outer join DB_T_PROD_STAG.cctl_occupancytype 

			on cc_incident.OccupancyType_stg=cctl_occupancytype.id_stg

		where	 cc_incident.nk_vehicle is not null 

			and claimnumber is not null 

			and insurable_key is not null 

		QUALIFY	ROW_NUMBER() OVER (

		PARTITION BY claimnumber,

			insurable_key 

		ORDER BY cc_incident.createtime DESC,cc_incident.updatetime DESC,

				cc_incident.ID DESC)=1
) SRC
)
);


-- Component exp_pass_through, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_through AS
(
SELECT
SQ_cc_incident.ClaimNumber as ClaimNumber,
SQ_cc_incident.TYPECODE2 as TYPECODE2,
SQ_cc_incident.Description as Description,
SQ_cc_incident.VehicleOperable as VehicleOperable,
SQ_cc_incident.LossEstimate as LossEstimate,
SQ_cc_incident.OwnerRetainingSalvage as OwnerRetainingSalvage,
SQ_cc_incident.OwnersPermission as OwnersPermission,
SQ_cc_incident.Name as Name,
SQ_cc_incident.SubroPotential_alfa as SubroPotential_alfa,
SQ_cc_incident.OdomRead as OdomRead,
SQ_cc_incident.TYPECODE as TYPECODE,
SQ_cc_incident.TYPECODE1 as TYPECODE1,
SQ_cc_incident.ID as ID,
SQ_cc_incident.TotalLoss as TotalLoss,
SQ_cc_incident.insurable_key as insurable_key,
CASE WHEN LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE */ IS NULL THEN ''UNK'' ELSE LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE */ END as out_asset_sb_type,
SQ_cc_incident.InsurableInterestCategory as InsurableInterestCategory,
CASE WHEN LKP_3.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN */ IS NULL THEN ''UNK'' ELSE LKP_4.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN */ END as out_clasfcn_cd,
CASE WHEN SQ_cc_incident.medicare_indicator IS NULL THEN ''UNK'' ELSE SQ_cc_incident.medicare_indicator END as o_TYPECODE,
SQ_cc_incident.VehicleSalvageAbandoned_alfa as VehicleSalvageAbandoned_alfa,
SQ_cc_incident.Speed as Speed,
SQ_cc_incident.TYPECODE_personrelationtype as TYPECODE_personrelationtype,
''GWCC'' as out_CLM_SRC_CD,
SQ_cc_incident.CreateTime as CreateTime,
SQ_cc_incident.UpdateTime as UpdateTime,
SQ_cc_incident.CLM_INSRBL_INT_TYPE as CLM_INSRBL_INT_TYPE,
SQ_cc_incident.Retired as Retired,
SQ_cc_incident.DateSalvageAssigned as DateSalvageAssigned,
SQ_cc_incident.VehStolenInd as VehStolenInd,
decode ( SQ_cc_incident.VehRecoveredInd_alfa , ''T'' , 1 , ''F'' , 0 ) as o_VehRecoveredInd_alfa,
SQ_cc_incident.source_record_id,
row_number() over (partition by SQ_cc_incident.source_record_id order by SQ_cc_incident.source_record_id) as RNK
FROM
SQ_cc_incident
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = SQ_cc_incident.PRTY_ASSET_SB_TYPE
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = SQ_cc_incident.PRTY_ASSET_SB_TYPE
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN LKP_3 ON LKP_3.SRC_IDNTFTN_VAL = SQ_cc_incident.clasfcn_cd
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN LKP_4 ON LKP_4.SRC_IDNTFTN_VAL = SQ_cc_incident.clasfcn_cd
QUALIFY RNK = 1
);


-- Component LKP_TERADATA_ETL_REF_XLAT_PERSONAL_RELATION, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_PERSONAL_RELATION AS
(
SELECT
LKP.TGT_IDNTFTN_VAL,
exp_pass_through.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_through.source_record_id ORDER BY LKP.TGT_IDNTFTN_VAL desc,LKP.SRC_IDNTFTN_VAL desc) RNK
FROM
exp_pass_through
LEFT JOIN (
SELECT 
	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL
	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 
FROM 
	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT
WHERE 
	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''PRSN_RLTN_TYPE'' 
             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''cctl_personrelationtype.typecode'' 
		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''GW'' 
		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
) LKP ON LKP.SRC_IDNTFTN_VAL = exp_pass_through.TYPECODE_personrelationtype
QUALIFY RNK = 1
);


-- Component LKP_PRTY_ASSET_ID, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_PRTY_ASSET_ID AS
(
SELECT
LKP.PRTY_ASSET_ID,
exp_pass_through.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_through.source_record_id ORDER BY LKP.PRTY_ASSET_ID asc,LKP.ASSET_HOST_ID_VAL asc,LKP.PRTY_ASSET_SBTYPE_CD asc,LKP.PRTY_ASSET_CLASFCN_CD asc,LKP.ASSET_INSRNC_HIST_TYPE_CD asc,LKP.ASSET_DESC asc,LKP.PRTY_ASSET_NAME asc,LKP.PRTY_ASSET_STRT_DTTM asc,LKP.PRTY_ASSET_END_DTTM asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc,LKP.SRC_SYS_CD asc) RNK
FROM
exp_pass_through
LEFT JOIN (
SELECT PRTY_ASSET.PRTY_ASSET_ID as PRTY_ASSET_ID, PRTY_ASSET.ASSET_INSRNC_HIST_TYPE_CD as ASSET_INSRNC_HIST_TYPE_CD, PRTY_ASSET.ASSET_DESC as ASSET_DESC, PRTY_ASSET.PRTY_ASSET_NAME as PRTY_ASSET_NAME, PRTY_ASSET.PRTY_ASSET_STRT_DTTM as PRTY_ASSET_STRT_DTTM, PRTY_ASSET.PRTY_ASSET_END_DTTM as PRTY_ASSET_END_DTTM, PRTY_ASSET.EDW_STRT_DTTM as EDW_STRT_DTTM, PRTY_ASSET.EDW_END_DTTM as EDW_END_DTTM, PRTY_ASSET.SRC_SYS_CD as SRC_SYS_CD, PRTY_ASSET.ASSET_HOST_ID_VAL as ASSET_HOST_ID_VAL, PRTY_ASSET.PRTY_ASSET_SBTYPE_CD as PRTY_ASSET_SBTYPE_CD, PRTY_ASSET.PRTY_ASSET_CLASFCN_CD as PRTY_ASSET_CLASFCN_CD 
FROM DB_T_PROD_CORE.PRTY_ASSET 
QUALIFY ROW_NUMBER() OVER(PARTITION BY  ASSET_HOST_ID_VAL,PRTY_ASSET_SBTYPE_CD,PRTY_ASSET_CLASFCN_CD ORDER BY EDW_END_DTTM DESC) = 1
) LKP ON LKP.ASSET_HOST_ID_VAL = exp_pass_through.insurable_key AND LKP.PRTY_ASSET_SBTYPE_CD = exp_pass_through.out_asset_sb_type AND LKP.PRTY_ASSET_CLASFCN_CD = exp_pass_through.out_clasfcn_cd
QUALIFY RNK = 1
);


-- Component LKP_CLM, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_CLM AS
(
SELECT
LKP.CLM_ID,
exp_pass_through.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_through.source_record_id ORDER BY LKP.CLM_ID desc,LKP.CLM_TYPE_CD desc,LKP.CLM_MDIA_TYPE_CD desc,LKP.CLM_SUBMTL_TYPE_CD desc,LKP.ACDNT_TYPE_CD desc,LKP.CLM_CTGY_TYPE_CD desc,LKP.ADDL_INSRNC_PLN_IND desc,LKP.EMPLMT_RLTD_IND desc,LKP.ATTNY_INVLVMT_IND desc,LKP.CLM_NUM desc,LKP.CLM_PRIR_IND desc,LKP.PMT_MODE_CD desc,LKP.CLM_OBLGTN_TYPE_CD desc,LKP.SUBRGTN_ELGBL_CD desc,LKP.SUBRGTN_ELGBLY_RSN_CD desc,LKP.CURY_CD desc,LKP.INCDT_EV_ID desc,LKP.INSRD_AT_FAULT_IND desc,LKP.CVGE_IN_QUES_IND desc,LKP.EXTNT_OF_FIRE_DMG_TYPE_CD desc,LKP.VFYD_CLM_IND desc,LKP.PRCS_ID desc,LKP.CLM_STRT_DTTM desc,LKP.CLM_END_DTTM desc,LKP.EDW_STRT_DTTM desc,LKP.EDW_END_DTTM desc,LKP.SRC_SYS_CD desc,LKP.TRANS_STRT_DTTM desc,LKP.LGCY_CLM_NUM desc) RNK
FROM
exp_pass_through
LEFT JOIN (
SELECT CLM.CLM_ID as CLM_ID, CLM.CLM_TYPE_CD as CLM_TYPE_CD, CLM.CLM_MDIA_TYPE_CD as CLM_MDIA_TYPE_CD, CLM.CLM_SUBMTL_TYPE_CD as CLM_SUBMTL_TYPE_CD, CLM.ACDNT_TYPE_CD as ACDNT_TYPE_CD, CLM.CLM_CTGY_TYPE_CD as CLM_CTGY_TYPE_CD, CLM.ADDL_INSRNC_PLN_IND as ADDL_INSRNC_PLN_IND, CLM.EMPLMT_RLTD_IND as EMPLMT_RLTD_IND, CLM.ATTNY_INVLVMT_IND as ATTNY_INVLVMT_IND, CLM.CLM_PRIR_IND as CLM_PRIR_IND, CLM.PMT_MODE_CD as PMT_MODE_CD, CLM.CLM_OBLGTN_TYPE_CD as CLM_OBLGTN_TYPE_CD, CLM.SUBRGTN_ELGBL_CD as SUBRGTN_ELGBL_CD, CLM.SUBRGTN_ELGBLY_RSN_CD as SUBRGTN_ELGBLY_RSN_CD, CLM.CURY_CD as CURY_CD, CLM.INCDT_EV_ID as INCDT_EV_ID, CLM.INSRD_AT_FAULT_IND as INSRD_AT_FAULT_IND, CLM.CVGE_IN_QUES_IND as CVGE_IN_QUES_IND, CLM.EXTNT_OF_FIRE_DMG_TYPE_CD as EXTNT_OF_FIRE_DMG_TYPE_CD, CLM.VFYD_CLM_IND as VFYD_CLM_IND, CLM.PRCS_ID as PRCS_ID, CLM.CLM_STRT_DTTM as CLM_STRT_DTTM, CLM.CLM_END_DTTM as CLM_END_DTTM, CLM.EDW_STRT_DTTM as EDW_STRT_DTTM, CLM.EDW_END_DTTM as EDW_END_DTTM, CLM.TRANS_STRT_DTTM as TRANS_STRT_DTTM, CLM.LGCY_CLM_NUM as LGCY_CLM_NUM, CLM.CLM_NUM as CLM_NUM, CLM.SRC_SYS_CD as SRC_SYS_CD FROM DB_T_PROD_CORE.CLM  QUALIFY ROW_NUMBER() OVER(PARTITION BY CLM.CLM_NUM,CLM.SRC_SYS_CD  ORDER BY CLM.EDW_END_DTTM desc) = 1
) LKP ON LKP.CLM_NUM = exp_pass_through.ClaimNumber AND LKP.SRC_SYS_CD = exp_pass_through.out_CLM_SRC_CD
QUALIFY RNK = 1
);


-- Component LKP_INSRBL_INT_PRTY_ASSET_ID, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_INSRBL_INT_PRTY_ASSET_ID AS
(
SELECT
LKP.INSRBL_INT_ID,
LKP.INSRBL_INT_CTGY_CD,
exp_pass_through.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_through.source_record_id ORDER BY LKP.INSRBL_INT_ID desc,LKP.INSRBL_INT_CTGY_CD desc,LKP.CTSTRPH_EXPSR_IND desc,LKP.SRC_SYS_CD desc,LKP.PRTY_ASSET_ID desc,LKP.EDW_STRT_DTTM desc,LKP.EDW_END_DTTM desc) RNK1
FROM
exp_pass_through
INNER JOIN LKP_PRTY_ASSET_ID ON exp_pass_through.source_record_id = LKP_PRTY_ASSET_ID.source_record_id
LEFT JOIN (
SELECT INSRBL_INT.INSRBL_INT_ID as INSRBL_INT_ID, INSRBL_INT.CTSTRPH_EXPSR_IND as CTSTRPH_EXPSR_IND, INSRBL_INT.EDW_STRT_DTTM as EDW_STRT_DTTM, INSRBL_INT.EDW_END_DTTM as EDW_END_DTTM, INSRBL_INT.INSRBL_INT_CTGY_CD as INSRBL_INT_CTGY_CD, INSRBL_INT.SRC_SYS_CD as SRC_SYS_CD, INSRBL_INT.PRTY_ASSET_ID as PRTY_ASSET_ID FROM DB_T_PROD_CORE.INSRBL_INT
 where  INSRBL_INT.INSRBL_INT_CTGY_CD = ''ASSET''
AND INSRBL_INT.PRTY_ASSET_ID IS NOT NULL 
qualify row_number () over (partition by INSRBL_INT_CTGY_CD,INSRBL_INT_ID order by EDW_END_DTTM desc)=1
) LKP ON LKP.INSRBL_INT_CTGY_CD = exp_pass_through.InsurableInterestCategory AND LKP.SRC_SYS_CD = exp_pass_through.out_CLM_SRC_CD AND LKP.PRTY_ASSET_ID = LKP_PRTY_ASSET_ID.PRTY_ASSET_ID
QUALIFY RNK1 = 1
);


-- Component LKP_INDIV_CLM_CTR, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_INDIV_CLM_CTR AS
(
SELECT
LKP.INDIV_PRTY_ID,
exp_pass_through.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_through.source_record_id ORDER BY LKP.INDIV_PRTY_ID desc,LKP.NK_PUBLC_ID desc) RNK
FROM
exp_pass_through
LEFT JOIN (
SELECT 
	INDIV.INDIV_PRTY_ID as INDIV_PRTY_ID, 
	INDIV.NK_PUBLC_ID as NK_PUBLC_ID 
FROM 
	DB_T_PROD_CORE.INDIV
WHERE
	INDIV.NK_PUBLC_ID IS NOT NULL
) LKP ON LKP.NK_PUBLC_ID = exp_pass_through.insurable_key
QUALIFY RNK = 1
);


-- Component LKP_INSRBL_INT_PRTYID, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_INSRBL_INT_PRTYID AS
(
SELECT
LKP.INSRBL_INT_ID,
LKP.INSRBL_INT_CTGY_CD,
exp_pass_through.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_through.source_record_id ORDER BY LKP.INSRBL_INT_ID desc,LKP.INSRBL_INT_CTGY_CD desc,LKP.CTSTRPH_EXPSR_IND desc,LKP.SRC_SYS_CD desc,LKP.INJURED_PRTY_ID desc,LKP.EDW_STRT_DTTM desc,LKP.EDW_END_DTTM desc) RNK1
FROM
exp_pass_through
INNER JOIN LKP_INDIV_CLM_CTR ON exp_pass_through.source_record_id = LKP_INDIV_CLM_CTR.source_record_id
LEFT JOIN (
SELECT INSRBL_INT.INSRBL_INT_ID as INSRBL_INT_ID, INSRBL_INT.CTSTRPH_EXPSR_IND as CTSTRPH_EXPSR_IND, INSRBL_INT.EDW_STRT_DTTM as EDW_STRT_DTTM, INSRBL_INT.EDW_END_DTTM as EDW_END_DTTM, INSRBL_INT.INSRBL_INT_CTGY_CD as INSRBL_INT_CTGY_CD, INSRBL_INT.SRC_SYS_CD as SRC_SYS_CD, INSRBL_INT.INJURED_PRTY_ID as INJURED_PRTY_ID FROM DB_T_PROD_CORE.INSRBL_INT
 where   INSRBL_INT.INSRBL_INT_CTGY_CD = ''PERSON''
AND  INSRBL_INT.INJURED_PRTY_ID  IS NOT NULL 
qualify row_number () over (partition by INSRBL_INT_CTGY_CD,INSRBL_INT_ID order by EDW_END_DTTM desc)=1
) LKP ON LKP.INSRBL_INT_CTGY_CD = exp_pass_through.InsurableInterestCategory AND LKP.SRC_SYS_CD = exp_pass_through.out_CLM_SRC_CD AND LKP.INJURED_PRTY_ID = LKP_INDIV_CLM_CTR.INDIV_PRTY_ID
QUALIFY RNK1 = 1
);


-- Component exp_cal, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_cal AS
(
SELECT
exp_pass_through.CLM_INSRBL_INT_TYPE as CLM_INSRBL_INT_TYPE,
CASE WHEN ( LKP_INSRBL_INT_PRTYID.INSRBL_INT_CTGY_CD = ''PERSON'' and LKP_INSRBL_INT_PRTYID.INSRBL_INT_ID IS NOT NULL ) THEN LKP_INSRBL_INT_PRTYID.INSRBL_INT_ID ELSE CASE WHEN LKP_INSRBL_INT_PRTY_ASSET_ID.INSRBL_INT_CTGY_CD = ''ASSET'' and LKP_INSRBL_INT_PRTY_ASSET_ID.INSRBL_INT_ID IS NOT NULL THEN LKP_INSRBL_INT_PRTY_ASSET_ID.INSRBL_INT_ID ELSE NULL END END as INSRBL_INT_ID,
LKP_CLM.CLM_ID as CLM_ID,
exp_pass_through.Description as Description,
CASE WHEN exp_pass_through.Description IS NULL or ltrim ( rtrim ( exp_pass_through.Description ) ) = '''' THEN ''UNK'' ELSE ltrim ( rtrim ( upper ( exp_pass_through.Description ) ) ) END as o_Description,
exp_pass_through.VehicleOperable as VehicleOperable,
CASE WHEN exp_pass_through.VehicleOperable IS NULL or ltrim ( rtrim ( exp_pass_through.VehicleOperable ) ) = '''' THEN ''UNK'' ELSE exp_pass_through.VehicleOperable END as o_VehicleOperable,
exp_pass_through.LossEstimate as LossEstimate,
exp_pass_through.OwnerRetainingSalvage as OwnerRetainingSalvage,
CASE WHEN exp_pass_through.OwnerRetainingSalvage IS NULL or ltrim ( rtrim ( exp_pass_through.OwnerRetainingSalvage ) ) = '''' THEN ''UNK'' ELSE exp_pass_through.OwnerRetainingSalvage END as o_OwnerRetainingSalvage,
exp_pass_through.OwnersPermission as OwnersPermission,
CASE WHEN exp_pass_through.OwnersPermission IS NULL or ltrim ( rtrim ( exp_pass_through.OwnersPermission ) ) = '''' THEN ''UNK'' ELSE exp_pass_through.OwnersPermission END as o_OwnersPermission,
exp_pass_through.SubroPotential_alfa as SubroPotential_alfa,
CASE WHEN exp_pass_through.SubroPotential_alfa IS NULL or ltrim ( rtrim ( exp_pass_through.SubroPotential_alfa ) ) = '''' THEN ''UNK'' ELSE exp_pass_through.SubroPotential_alfa END as o_SubroPotential_alfa,
exp_pass_through.OdomRead as OdomRead,
exp_pass_through.Name as Name,
CASE WHEN exp_pass_through.Name IS NULL or ltrim ( rtrim ( exp_pass_through.Name ) ) = '''' THEN ''UNK'' ELSE exp_pass_through.Name END as o_Name,
exp_pass_through.TYPECODE as TYPECODE_occupancytype,
CASE WHEN exp_pass_through.TYPECODE2 IS NULL or ltrim ( rtrim ( exp_pass_through.TYPECODE2 ) ) = '''' THEN ''UNK'' ELSE exp_pass_through.TYPECODE2 END as o_TYPECODE_yesno,
exp_pass_through.TYPECODE1 as TYPECODE_severitytype,
exp_pass_through.ID as ID,
exp_pass_through.TotalLoss as TotalLoss,
CASE WHEN exp_pass_through.TotalLoss IS NULL or ltrim ( rtrim ( exp_pass_through.TotalLoss ) ) = '''' THEN ''UNK'' ELSE exp_pass_through.TotalLoss END as o_TotalLoss,
CASE WHEN exp_pass_through.o_TYPECODE IS NULL or ltrim ( rtrim ( exp_pass_through.o_TYPECODE ) ) = '''' THEN ''UNK'' ELSE UPPER ( ltrim ( rtrim ( exp_pass_through.o_TYPECODE ) ) ) END as o_MEDCR_IND,
exp_pass_through.VehicleSalvageAbandoned_alfa as VehicleSalvageAbandoned_alfa,
CASE WHEN exp_pass_through.VehicleSalvageAbandoned_alfa IS NULL or ltrim ( rtrim ( exp_pass_through.VehicleSalvageAbandoned_alfa ) ) = '''' THEN ''UNK'' ELSE UPPER ( exp_pass_through.VehicleSalvageAbandoned_alfa ) END as o_VehicleSalvageAbandoned_alfa,
exp_pass_through.Speed as Speed,
exp_pass_through.CreateTime as CreateTime,
exp_pass_through.UpdateTime as UpdateTime,
CURRENT_TIMESTAMP as in_EDW_STRT_DTTM,
CASE WHEN LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_CLM_INSRBL_INT_TYPE */ IS NULL THEN ''UNK'' ELSE LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_CLM_INSRBL_INT_TYPE */ END as v_clm_ins_int_typecode,
CASE WHEN LKP_TERADATA_ETL_REF_XLAT_PERSONAL_RELATION.TGT_IDNTFTN_VAL IS NULL THEN ''UNK'' ELSE LKP_TERADATA_ETL_REF_XLAT_PERSONAL_RELATION.TGT_IDNTFTN_VAL END as o_TGT_IDNTFTN_VAL,
exp_pass_through.Retired as Retired,
exp_pass_through.DateSalvageAssigned as DateSalvageAssigned,
exp_pass_through.VehStolenInd as VehStolenInd,
exp_pass_through.o_VehRecoveredInd_alfa as VehRecoveredInd_alfa,
exp_pass_through.source_record_id,
row_number() over (partition by exp_pass_through.source_record_id order by exp_pass_through.source_record_id) as RNK2
FROM
exp_pass_through
INNER JOIN LKP_TERADATA_ETL_REF_XLAT_PERSONAL_RELATION ON exp_pass_through.source_record_id = LKP_TERADATA_ETL_REF_XLAT_PERSONAL_RELATION.source_record_id
INNER JOIN LKP_CLM ON LKP_TERADATA_ETL_REF_XLAT_PERSONAL_RELATION.source_record_id = LKP_CLM.source_record_id
INNER JOIN LKP_INSRBL_INT_PRTY_ASSET_ID ON LKP_CLM.source_record_id = LKP_INSRBL_INT_PRTY_ASSET_ID.source_record_id
INNER JOIN LKP_INSRBL_INT_PRTYID ON LKP_INSRBL_INT_PRTY_ASSET_ID.source_record_id = LKP_INSRBL_INT_PRTYID.source_record_id
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_CLM_INSRBL_INT_TYPE LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = exp_pass_through.CLM_INSRBL_INT_TYPE
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_CLM_INSRBL_INT_TYPE LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = exp_pass_through.CLM_INSRBL_INT_TYPE
QUALIFY RNK2 = 1
);


-- Component LKP_TERADATA_ETL_REF_XLAT1, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT1 AS
(
SELECT
LKP.TGT_IDNTFTN_VAL,
exp_cal.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_cal.source_record_id ORDER BY LKP.TGT_IDNTFTN_VAL desc,LKP.SRC_IDNTFTN_VAL desc) RNK
FROM
exp_cal
LEFT JOIN (
SELECT 
	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL
	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 
FROM 
	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT
WHERE 
	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''INJRY_SVRTY_TYPE'' 
             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''cctl_severitytype.TYPECODE'' 
		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''GW'' 
		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
) LKP ON LKP.SRC_IDNTFTN_VAL = exp_cal.TYPECODE_severitytype
QUALIFY RNK = 1
);


-- Component LKP_CLM_INSRBL_INT, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_CLM_INSRBL_INT AS
(
SELECT
LKP.CLM_ID,
LKP.INSRBL_INT_ID,
LKP.CLM_INSRBL_INT_TYPE_CD,
LKP.ATTNY_RPRSNTD_IND,
LKP.CLM_INSRBL_INT_DESC_TXT,
LKP.VEH_OPRTNL_IND,
LKP.ESTMTD_LOSS_AMT,
LKP.OWNR_RETAINING_SALV_VEH_IND,
LKP.SOLD_OUTSD_SALV_POOL_YD_IND,
LKP.SALV_VEH_ABNDN_IND,
LKP.OWNR_PRMSSN_IND,
LKP.SUBRGTN_POTEN_IND,
LKP.ODMTR_READG_VAL,
LKP.OCCPY_TYPE_CD,
LKP.INJRY_SVRTY_TYPE_CD,
LKP.VEH_TOT_LOSS_IND,
LKP.CLM_UNIT_NUM,
LKP.MEDCR_IND,
LKP.PRSN_RLTN_TYPE_CD,
LKP.CLM_EXPSR_INCDT_VEH_STLN_IND,
LKP.CLM_EXPSR_INCDT_VEH_RCVRD_IND,
LKP.VEH_DT_ASGND_TO_SALV,
LKP.CLM_INSRBL_INT_STRT_DTTM,
LKP.EDW_STRT_DTTM,
LKP.EDW_END_DTTM,
exp_cal.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_cal.source_record_id ORDER BY LKP.CLM_ID asc,LKP.INSRBL_INT_ID asc,LKP.CLM_INSRBL_INT_TYPE_CD asc,LKP.ATTNY_RPRSNTD_IND asc,LKP.CLM_INSRBL_INT_DESC_TXT asc,LKP.VEH_OPRTNL_IND asc,LKP.ESTMTD_LOSS_AMT asc,LKP.OWNR_RETAINING_SALV_VEH_IND asc,LKP.SOLD_OUTSD_SALV_POOL_YD_IND asc,LKP.SALV_VEH_ABNDN_IND asc,LKP.OWNR_PRMSSN_IND asc,LKP.SUBRGTN_POTEN_IND asc,LKP.ODMTR_READG_VAL asc,LKP.OCCPY_TYPE_CD asc,LKP.INJRY_SVRTY_TYPE_CD asc,LKP.VEH_TOT_LOSS_IND asc,LKP.CLM_UNIT_NUM asc,LKP.MEDCR_IND asc,LKP.PRSN_RLTN_TYPE_CD asc,LKP.CLM_EXPSR_INCDT_VEH_STLN_IND asc,LKP.CLM_EXPSR_INCDT_VEH_RCVRD_IND asc,LKP.VEH_DT_ASGND_TO_SALV asc,LKP.CLM_INSRBL_INT_STRT_DTTM asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc) RNK
FROM
exp_cal
LEFT JOIN (
SELECT CLM_INSRBL_INT.CLM_INSRBL_INT_TYPE_CD as CLM_INSRBL_INT_TYPE_CD, CLM_INSRBL_INT.ATTNY_RPRSNTD_IND as ATTNY_RPRSNTD_IND, CLM_INSRBL_INT.CLM_INSRBL_INT_DESC_TXT as CLM_INSRBL_INT_DESC_TXT, CLM_INSRBL_INT.VEH_OPRTNL_IND as VEH_OPRTNL_IND, CLM_INSRBL_INT.ESTMTD_LOSS_AMT as ESTMTD_LOSS_AMT, CLM_INSRBL_INT.OWNR_RETAINING_SALV_VEH_IND as OWNR_RETAINING_SALV_VEH_IND, CLM_INSRBL_INT.SOLD_OUTSD_SALV_POOL_YD_IND as SOLD_OUTSD_SALV_POOL_YD_IND, CLM_INSRBL_INT.SALV_VEH_ABNDN_IND as SALV_VEH_ABNDN_IND, CLM_INSRBL_INT.OWNR_PRMSSN_IND as OWNR_PRMSSN_IND, CLM_INSRBL_INT.SUBRGTN_POTEN_IND as SUBRGTN_POTEN_IND, CLM_INSRBL_INT.ODMTR_READG_VAL as ODMTR_READG_VAL, CLM_INSRBL_INT.OCCPY_TYPE_CD as OCCPY_TYPE_CD, CLM_INSRBL_INT.INJRY_SVRTY_TYPE_CD as INJRY_SVRTY_TYPE_CD, CLM_INSRBL_INT.VEH_TOT_LOSS_IND as VEH_TOT_LOSS_IND, CLM_INSRBL_INT.CLM_UNIT_NUM as CLM_UNIT_NUM, CLM_INSRBL_INT.MEDCR_IND as MEDCR_IND, CLM_INSRBL_INT.PRSN_RLTN_TYPE_CD as PRSN_RLTN_TYPE_CD, CLM_INSRBL_INT.CLM_EXPSR_INCDT_VEH_STLN_IND as CLM_EXPSR_INCDT_VEH_STLN_IND, CLM_INSRBL_INT.CLM_EXPSR_INCDT_VEH_RCVRD_IND as CLM_EXPSR_INCDT_VEH_RCVRD_IND, CLM_INSRBL_INT.VEH_DT_ASGND_TO_SALV as VEH_DT_ASGND_TO_SALV, CLM_INSRBL_INT.CLM_INSRBL_INT_STRT_DTTM as CLM_INSRBL_INT_STRT_DTTM, CLM_INSRBL_INT.EDW_STRT_DTTM as EDW_STRT_DTTM, CLM_INSRBL_INT.EDW_END_DTTM as EDW_END_DTTM, CLM_INSRBL_INT.CLM_ID as CLM_ID, CLM_INSRBL_INT.INSRBL_INT_ID as INSRBL_INT_ID FROM DB_T_PROD_CORE.CLM_INSRBL_INT QUALIFY ROW_NUMBER() OVER(PARTITION BY CLM_INSRBL_INT.CLM_ID,CLM_INSRBL_INT.INSRBL_INT_ID ORDER BY CLM_INSRBL_INT.EDW_END_DTTM desc) = 1
) LKP ON LKP.CLM_ID = exp_cal.CLM_ID AND LKP.INSRBL_INT_ID = exp_cal.INSRBL_INT_ID
QUALIFY RNK = 1
);


-- Component exp_pass_lkp, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_lkp AS
(
SELECT
CASE WHEN LKP_TERADATA_ETL_REF_XLAT1.TGT_IDNTFTN_VAL IS NULL THEN ''UNK'' ELSE LKP_TERADATA_ETL_REF_XLAT1.TGT_IDNTFTN_VAL END as TGT_REF_TYPE_CD_occ_out,
CASE WHEN LKP_TERADATA_ETL_REF_XLAT1.TGT_IDNTFTN_VAL IS NULL THEN ''UNK'' ELSE LKP_TERADATA_ETL_REF_XLAT1.TGT_IDNTFTN_VAL END as TGT_REF_TYPE_CD_sev_out,
exp_cal.CreateTime as CreateTime,
CASE WHEN exp_cal.CreateTime IS NULL THEN to_date ( ''1900-01-01'' , ''YYYY-MM-DD'' ) ELSE exp_cal.CreateTime END as o_CreateTime,
exp_cal.UpdateTime as UpdateTime,
exp_cal.source_record_id
FROM
exp_cal
INNER JOIN LKP_TERADATA_ETL_REF_XLAT1 ON exp_cal.source_record_id = LKP_TERADATA_ETL_REF_XLAT1.source_record_id
--INNER JOIN LKP_TERADATA_ETL_REF_XLAT ON LKP_TERADATA_ETL_REF_XLAT1.source_record_id = LKP_TERADATA_ETL_REF_XLAT.source_record_id
);


-- Component exp_pass_tgt, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_tgt AS
(
SELECT
exp_cal.INSRBL_INT_ID as INSRBL_INT_ID1,
exp_cal.CLM_ID as CLM_ID1,
exp_cal.o_Description as Description,
exp_cal.o_VehicleOperable as VehicleOperable,
exp_cal.LossEstimate as LossEstimate,
exp_cal.o_OwnerRetainingSalvage as OwnerRetainingSalvage,
exp_cal.o_OwnersPermission as OwnersPermission,
exp_cal.o_SubroPotential_alfa as SubroPotential_alfa,
exp_cal.OdomRead as OdomRead,
exp_cal.o_Name as Name,
exp_pass_lkp.TGT_REF_TYPE_CD_occ_out as TGT_REF_TYPE_CD_occ_out,
exp_pass_lkp.TGT_REF_TYPE_CD_sev_out as TGT_REF_TYPE_CD_sev_out,
exp_cal.o_TYPECODE_yesno as TYPECODE_yesno,
exp_cal.CLM_INSRBL_INT_TYPE as CLM_INSRBL_INT_TYPE,
exp_cal.ID as ID,
:PRCS_ID as prcs_id,
CASE WHEN LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_CLM_INSRBL_INT_TYPE */ IS NULL THEN ''UNK'' ELSE LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_CLM_INSRBL_INT_TYPE */ END as v_clm_ins_int_typecode,
v_clm_ins_int_typecode as o_clm_ins_int_typecode,
exp_cal.o_TotalLoss as TotalLoss,
exp_cal.o_MEDCR_IND as TYPECODE3,
exp_cal.o_VehicleSalvageAbandoned_alfa as VehicleSalvageAbandoned_alfa,
exp_cal.Speed as Speed,
exp_cal.o_TGT_IDNTFTN_VAL as out_TGT_IDNTFTN_VAL_personal_relation,
LKP_CLM_INSRBL_INT.EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
LKP_CLM_INSRBL_INT.EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_cal.in_EDW_STRT_DTTM as EDW_STRT_DTTM,
TO_TIMESTAMP ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as EDW_END_DTTM,
DATEADD (SECOND, -1, CURRENT_TIMESTAMP()) as EDW_END_DTTM_exp,
exp_pass_lkp.o_CreateTime as in_CLM_INSRBL_INT_STRT_DT,
exp_pass_lkp.UpdateTime as in_TRANS_STRT_DTTM,
LKP_CLM_INSRBL_INT.CLM_INSRBL_INT_STRT_DTTM as LKP_CLM_INSRBL_INT_STRT_DT,
exp_cal.Retired as Retired,
to_char ( exp_cal.DateSalvageAssigned , ''yyyy-mm-dd'' ) as v_dateSalvageAssigned,
to_date ( v_dateSalvageAssigned , ''yyyy-mm-dd'' ) as o_dateSalvageAssigned,
o_dateSalvageAssigned as o_dateSalvgeAssigned,
exp_cal.VehStolenInd as VehStolenInd,
exp_cal.VehRecoveredInd_alfa as VehRecoveredInd_alfa,
CURRENT_TIMESTAMP as out_start_dt,
to_timestamp( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) as o_TRANS_END_DTTM,
md5 ( ltrim ( rtrim ( exp_cal.o_Description ) ) || ltrim ( rtrim ( exp_cal.o_VehicleOperable ) ) || ltrim ( rtrim ( exp_cal.o_OwnerRetainingSalvage ) ) || ltrim ( rtrim ( exp_cal.o_OwnersPermission ) ) || ltrim ( rtrim ( exp_cal.o_SubroPotential_alfa ) ) || ltrim ( rtrim ( exp_cal.OdomRead ) ) || ltrim ( rtrim ( exp_cal.o_Name ) ) || ltrim ( rtrim ( exp_pass_lkp.TGT_REF_TYPE_CD_occ_out ) ) || ltrim ( rtrim ( exp_pass_lkp.TGT_REF_TYPE_CD_sev_out ) ) || ltrim ( rtrim ( exp_cal.o_TotalLoss ) ) || ltrim ( rtrim ( exp_cal.o_VehicleSalvageAbandoned_alfa ) ) || ltrim ( rtrim ( exp_cal.o_TGT_IDNTFTN_VAL ) ) || ltrim ( rtrim ( exp_cal.o_TYPECODE_yesno ) ) || to_char ( ltrim ( rtrim ( exp_cal.ID ) ) ) || ltrim ( rtrim ( exp_cal.o_MEDCR_IND ) ) || to_char ( ltrim ( rtrim ( exp_cal.LossEstimate ) ) ) || to_char ( ltrim ( rtrim ( exp_pass_lkp.o_CreateTime ) ) ) || ltrim ( rtrim ( v_clm_ins_int_typecode ) ) || to_char ( ltrim ( rtrim ( o_dateSalvageAssigned ) ) ) || ltrim ( rtrim ( exp_cal.VehStolenInd ) ) || to_char ( ltrim ( rtrim ( exp_cal.VehRecoveredInd_alfa ) ) ) ) as chksum_src,
md5 ( ltrim ( rtrim ( LKP_CLM_INSRBL_INT.CLM_INSRBL_INT_DESC_TXT ) ) || ltrim ( rtrim ( LKP_CLM_INSRBL_INT.VEH_OPRTNL_IND ) ) || ltrim ( rtrim ( LKP_CLM_INSRBL_INT.OWNR_RETAINING_SALV_VEH_IND ) ) || ltrim ( rtrim ( LKP_CLM_INSRBL_INT.OWNR_PRMSSN_IND ) ) || ltrim ( rtrim ( LKP_CLM_INSRBL_INT.SUBRGTN_POTEN_IND ) ) || ltrim ( rtrim ( LKP_CLM_INSRBL_INT.ODMTR_READG_VAL ) ) || ltrim ( rtrim ( LKP_CLM_INSRBL_INT.SOLD_OUTSD_SALV_POOL_YD_IND ) ) || ltrim ( rtrim ( LKP_CLM_INSRBL_INT.OCCPY_TYPE_CD ) ) || ltrim ( rtrim ( LKP_CLM_INSRBL_INT.INJRY_SVRTY_TYPE_CD ) ) || ltrim ( rtrim ( LKP_CLM_INSRBL_INT.VEH_TOT_LOSS_IND ) ) || ltrim ( rtrim ( LKP_CLM_INSRBL_INT.SALV_VEH_ABNDN_IND ) ) || ltrim ( rtrim ( LKP_CLM_INSRBL_INT.PRSN_RLTN_TYPE_CD ) ) || ltrim ( rtrim ( LKP_CLM_INSRBL_INT.ATTNY_RPRSNTD_IND ) ) || to_char ( ltrim ( rtrim ( LKP_CLM_INSRBL_INT.CLM_UNIT_NUM ) ) ) || ltrim ( rtrim ( LKP_CLM_INSRBL_INT.MEDCR_IND ) ) || to_char ( ltrim ( rtrim ( LKP_CLM_INSRBL_INT.ESTMTD_LOSS_AMT ) ) ) || to_char ( ltrim ( rtrim ( LKP_CLM_INSRBL_INT.CLM_INSRBL_INT_STRT_DTTM ) ) ) || ltrim ( rtrim ( LKP_CLM_INSRBL_INT.CLM_INSRBL_INT_TYPE_CD ) ) || to_char ( ltrim ( rtrim ( LKP_CLM_INSRBL_INT.VEH_DT_ASGND_TO_SALV ) ) ) || ltrim ( rtrim ( LKP_CLM_INSRBL_INT.CLM_EXPSR_INCDT_VEH_STLN_IND ) ) || to_char ( ltrim ( rtrim ( LKP_CLM_INSRBL_INT.CLM_EXPSR_INCDT_VEH_RCVRD_IND ) ) ) ) as chksum_lkp,
CASE WHEN chksum_lkp IS NULL THEN ''I'' ELSE CASE WHEN chksum_src != chksum_lkp THEN ''U'' ELSE ''R'' END END as o_flag,
exp_cal.source_record_id,
row_number() over (partition by exp_cal.source_record_id order by exp_cal.source_record_id) as RNK
FROM
exp_cal
INNER JOIN LKP_CLM_INSRBL_INT ON exp_cal.source_record_id = LKP_CLM_INSRBL_INT.source_record_id
INNER JOIN exp_pass_lkp ON LKP_CLM_INSRBL_INT.source_record_id = exp_pass_lkp.source_record_id
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_CLM_INSRBL_INT_TYPE LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = exp_cal.CLM_INSRBL_INT_TYPE
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_CLM_INSRBL_INT_TYPE LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = exp_cal.CLM_INSRBL_INT_TYPE
QUALIFY RNK = 1
);


-- Component rtr_ins_upd_insert, Type ROUTER Output Group insert
CREATE OR REPLACE TEMPORARY TABLE rtr_ins_upd_insert AS
(
SELECT
exp_pass_tgt.INSRBL_INT_ID1 as INSRBL_INT_ID1,
exp_pass_tgt.CLM_ID1 as CLM_ID1,
exp_pass_tgt.Description as Description,
exp_pass_tgt.VehicleOperable as VehicleOperable,
exp_pass_tgt.LossEstimate as LossEstimate,
exp_pass_tgt.OwnerRetainingSalvage as OwnerRetainingSalvage,
exp_pass_tgt.OwnersPermission as OwnersPermission,
exp_pass_tgt.SubroPotential_alfa as SubroPotential_alfa,
exp_pass_tgt.OdomRead as OdomRead,
exp_pass_tgt.Name as Name,
exp_pass_tgt.TGT_REF_TYPE_CD_occ_out as TGT_REF_TYPE_CD_occ_out,
exp_pass_tgt.TGT_REF_TYPE_CD_sev_out as TGT_REF_TYPE_CD_sev_out,
exp_pass_tgt.TYPECODE_yesno as TYPECODE_yesno,
exp_pass_tgt.ID as ID,
exp_pass_tgt.prcs_id as prcs_id,
exp_pass_tgt.o_clm_ins_int_typecode as clm_ins_int_typecode,
exp_pass_tgt.TotalLoss as TotalLoss,
exp_pass_tgt.TYPECODE3 as TYPECODE3,
exp_pass_tgt.VehicleSalvageAbandoned_alfa as VehicleSalvageAbandoned_alfa,
exp_pass_tgt.Speed as Speed,
exp_pass_tgt.out_TGT_IDNTFTN_VAL_personal_relation as TGT_IDNTFTN_VAL_personal_relation,
exp_pass_tgt.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_pass_tgt.EDW_END_DTTM as EDW_END_DTTM,
exp_pass_tgt.EDW_END_DTTM_exp as EDW_END_DTTM_exp,
exp_pass_tgt.o_flag as o_flag,
exp_pass_tgt.out_start_dt as out_start_dt,
exp_pass_tgt.in_CLM_INSRBL_INT_STRT_DT as in_CLM_INSRBL_INT_STRT_DT,
exp_pass_tgt.in_TRANS_STRT_DTTM as in_TRANS_STRT_DTTM,
exp_pass_tgt.Retired as Retired,
exp_pass_tgt.o_dateSalvgeAssigned as DateSalvageAssigned,
exp_pass_tgt.VehStolenInd as VehStolenInd,
exp_pass_tgt.o_TRANS_END_DTTM as TRANS_END_DTTM,
exp_pass_tgt.VehRecoveredInd_alfa as VehRecoveredInd_alfa,
exp_pass_tgt.lkp_EDW_END_DTTM as lkp_EDW_END_DTMM,
exp_pass_tgt.LKP_CLM_INSRBL_INT_STRT_DT as lkp_CLM_INSRBL_INT_STRT_DTTM,
exp_pass_tgt.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_pass_tgt.source_record_id
FROM
exp_pass_tgt
WHERE exp_pass_tgt.CLM_ID1 IS NOT NULL AND exp_pass_tgt.INSRBL_INT_ID1 IS NOT NULL AND ( ( exp_pass_tgt.o_flag = ''I'' ) OR ( exp_pass_tgt.Retired = 0 AND exp_pass_tgt.lkp_EDW_END_DTTM != TO_timestamp( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ) OR ( exp_pass_tgt.o_flag = ''U'' AND exp_pass_tgt.lkp_EDW_END_DTTM = TO_timestamp ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ) ));


-- Component rtr_ins_upd_retired, Type ROUTER Output Group retired
CREATE OR REPLACE TEMPORARY TABLE rtr_ins_upd_retired AS
(
SELECT
exp_pass_tgt.INSRBL_INT_ID1 as INSRBL_INT_ID1,
exp_pass_tgt.CLM_ID1 as CLM_ID1,
exp_pass_tgt.Description as Description,
exp_pass_tgt.VehicleOperable as VehicleOperable,
exp_pass_tgt.LossEstimate as LossEstimate,
exp_pass_tgt.OwnerRetainingSalvage as OwnerRetainingSalvage,
exp_pass_tgt.OwnersPermission as OwnersPermission,
exp_pass_tgt.SubroPotential_alfa as SubroPotential_alfa,
exp_pass_tgt.OdomRead as OdomRead,
exp_pass_tgt.Name as Name,
exp_pass_tgt.TGT_REF_TYPE_CD_occ_out as TGT_REF_TYPE_CD_occ_out,
exp_pass_tgt.TGT_REF_TYPE_CD_sev_out as TGT_REF_TYPE_CD_sev_out,
exp_pass_tgt.TYPECODE_yesno as TYPECODE_yesno,
exp_pass_tgt.ID as ID,
exp_pass_tgt.prcs_id as prcs_id,
exp_pass_tgt.o_clm_ins_int_typecode as clm_ins_int_typecode,
exp_pass_tgt.TotalLoss as TotalLoss,
exp_pass_tgt.TYPECODE3 as TYPECODE3,
exp_pass_tgt.VehicleSalvageAbandoned_alfa as VehicleSalvageAbandoned_alfa,
exp_pass_tgt.Speed as Speed,
exp_pass_tgt.out_TGT_IDNTFTN_VAL_personal_relation as TGT_IDNTFTN_VAL_personal_relation,
exp_pass_tgt.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_pass_tgt.EDW_END_DTTM as EDW_END_DTTM,
exp_pass_tgt.EDW_END_DTTM_exp as EDW_END_DTTM_exp,
exp_pass_tgt.o_flag as o_flag,
exp_pass_tgt.out_start_dt as out_start_dt,
exp_pass_tgt.in_CLM_INSRBL_INT_STRT_DT as in_CLM_INSRBL_INT_STRT_DT,
exp_pass_tgt.in_TRANS_STRT_DTTM as in_TRANS_STRT_DTTM,
exp_pass_tgt.Retired as Retired,
exp_pass_tgt.o_dateSalvgeAssigned as DateSalvageAssigned,
exp_pass_tgt.VehStolenInd as VehStolenInd,
exp_pass_tgt.o_TRANS_END_DTTM as TRANS_END_DTTM,
exp_pass_tgt.VehRecoveredInd_alfa as VehRecoveredInd_alfa,
exp_pass_tgt.lkp_EDW_END_DTTM as lkp_EDW_END_DTMM,
exp_pass_tgt.LKP_CLM_INSRBL_INT_STRT_DT as lkp_CLM_INSRBL_INT_STRT_DTTM,
exp_pass_tgt.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_pass_tgt.source_record_id
FROM
exp_pass_tgt
WHERE exp_pass_tgt.o_flag = ''R'' and exp_pass_tgt.Retired != 0 and exp_pass_tgt.lkp_EDW_END_DTTM = TO_timestamp( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ));


-- Component upd_CLM_INSRBL_INT_insupd, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_CLM_INSRBL_INT_insupd AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_ins_upd_insert.INSRBL_INT_ID1 as INSRBL_INT_ID11,
rtr_ins_upd_insert.CLM_ID1 as CLM_ID11,
rtr_ins_upd_insert.Description as Description1,
rtr_ins_upd_insert.VehicleOperable as VehicleOperable1,
rtr_ins_upd_insert.LossEstimate as LossEstimate1,
rtr_ins_upd_insert.OwnerRetainingSalvage as OwnerRetainingSalvage1,
rtr_ins_upd_insert.OwnersPermission as OwnersPermission1,
rtr_ins_upd_insert.SubroPotential_alfa as SubroPotential_alfa1,
rtr_ins_upd_insert.OdomRead as OdomRead1,
rtr_ins_upd_insert.Name as Name1,
rtr_ins_upd_insert.TGT_REF_TYPE_CD_occ_out as TGT_REF_TYPE_CD_occ_out1,
rtr_ins_upd_insert.TGT_REF_TYPE_CD_sev_out as TGT_REF_TYPE_CD_sev_out1,
rtr_ins_upd_insert.TYPECODE_yesno as TYPECODE_yesno1,
rtr_ins_upd_insert.ID as ID1,
rtr_ins_upd_insert.prcs_id as prcs_id1,
rtr_ins_upd_insert.clm_ins_int_typecode as clm_ins_int_typecode1,
rtr_ins_upd_insert.TotalLoss as TotalLoss1,
rtr_ins_upd_insert.TYPECODE3 as TYPECODE31,
rtr_ins_upd_insert.VehicleSalvageAbandoned_alfa as VehicleSalvageAbandoned_alfa1,
rtr_ins_upd_insert.Speed as Speed1,
rtr_ins_upd_insert.TGT_IDNTFTN_VAL_personal_relation as TGT_IDNTFTN_VAL_personal_relation1,
rtr_ins_upd_insert.EDW_STRT_DTTM as EDW_STRT_DTTM1,
rtr_ins_upd_insert.EDW_END_DTTM as EDW_END_DTTM1,
rtr_ins_upd_insert.EDW_END_DTTM_exp as EDW_END_DTTM_exp1,
rtr_ins_upd_insert.o_flag as o_flag1,
rtr_ins_upd_insert.out_start_dt as out_start_dt1,
rtr_ins_upd_insert.in_CLM_INSRBL_INT_STRT_DT as in_CLM_INSRBL_INT_STRT_DT1,
rtr_ins_upd_insert.in_TRANS_STRT_DTTM as in_TRANS_STRT_DTTM1,
rtr_ins_upd_insert.Retired as Retired1,
rtr_ins_upd_insert.DateSalvageAssigned as DateSalvageAssigned1,
rtr_ins_upd_insert.VehStolenInd as VehStolenInd1,
rtr_ins_upd_insert.TRANS_END_DTTM as TRANS_END_DTTM1,
rtr_ins_upd_insert.VehRecoveredInd_alfa as VehRecoveredInd_alfa1,
rtr_ins_upd_insert.lkp_EDW_END_DTMM as lkp_EDW_END_DTMM1,
0 as UPDATE_STRATEGY_ACTION,
rtr_ins_upd_insert.source_record_id
FROM
rtr_ins_upd_insert
);


-- Component Exp_clm_insrbl_int_ins, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE Exp_clm_insrbl_int_ins AS
(
SELECT
upd_CLM_INSRBL_INT_insupd.CLM_ID11 as CLM_ID11,
upd_CLM_INSRBL_INT_insupd.INSRBL_INT_ID11 as INSRBL_INT_ID11,
upd_CLM_INSRBL_INT_insupd.Description1 as Description1,
upd_CLM_INSRBL_INT_insupd.VehicleOperable1 as VehicleOperable1,
upd_CLM_INSRBL_INT_insupd.LossEstimate1 as LossEstimate1,
upd_CLM_INSRBL_INT_insupd.OwnerRetainingSalvage1 as OwnerRetainingSalvage1,
upd_CLM_INSRBL_INT_insupd.OwnersPermission1 as OwnersPermission1,
upd_CLM_INSRBL_INT_insupd.SubroPotential_alfa1 as SubroPotential_alfa1,
upd_CLM_INSRBL_INT_insupd.OdomRead1 as OdomRead1,
upd_CLM_INSRBL_INT_insupd.Name1 as Name1,
upd_CLM_INSRBL_INT_insupd.TGT_REF_TYPE_CD_occ_out1 as TGT_REF_TYPE_CD_occ_out1,
upd_CLM_INSRBL_INT_insupd.TGT_REF_TYPE_CD_sev_out1 as TGT_REF_TYPE_CD_sev_out1,
upd_CLM_INSRBL_INT_insupd.TYPECODE_yesno1 as TYPECODE_yesno1,
upd_CLM_INSRBL_INT_insupd.ID1 as ID1,
upd_CLM_INSRBL_INT_insupd.prcs_id1 as prcs_id1,
upd_CLM_INSRBL_INT_insupd.clm_ins_int_typecode1 as clm_ins_int_typecode1,
upd_CLM_INSRBL_INT_insupd.TotalLoss1 as TotalLoss1,
upd_CLM_INSRBL_INT_insupd.TYPECODE31 as TYPECODE31,
upd_CLM_INSRBL_INT_insupd.VehicleSalvageAbandoned_alfa1 as VehicleSalvageAbandoned_alfa1,
upd_CLM_INSRBL_INT_insupd.Speed1 as Speed1,
upd_CLM_INSRBL_INT_insupd.TGT_IDNTFTN_VAL_personal_relation1 as TGT_IDNTFTN_VAL_personal_relation1,
upd_CLM_INSRBL_INT_insupd.EDW_STRT_DTTM1 as EDW_STRT_DTTM1,
upd_CLM_INSRBL_INT_insupd.in_CLM_INSRBL_INT_STRT_DT1 as in_CLM_INSRBL_INT_STRT_DT1,
upd_CLM_INSRBL_INT_insupd.in_TRANS_STRT_DTTM1 as in_TRANS_STRT_DTTM1,
upd_CLM_INSRBL_INT_insupd.DateSalvageAssigned1 as DateSalvageAssigned1,
upd_CLM_INSRBL_INT_insupd.VehStolenInd1 as VehStolenInd1,
upd_CLM_INSRBL_INT_insupd.VehRecoveredInd_alfa1 as VehRecoveredInd_alfa1,
CASE WHEN upd_CLM_INSRBL_INT_insupd.Retired1 != 0 THEN upd_CLM_INSRBL_INT_insupd.EDW_STRT_DTTM1 ELSE upd_CLM_INSRBL_INT_insupd.EDW_END_DTTM1 END as out_EDW_END_DTTM,
CASE WHEN upd_CLM_INSRBL_INT_insupd.Retired1 <> 0 THEN upd_CLM_INSRBL_INT_insupd.in_TRANS_STRT_DTTM1 ELSE upd_CLM_INSRBL_INT_insupd.TRANS_END_DTTM1 END as out_TRANS_END_DTTM,
upd_CLM_INSRBL_INT_insupd.source_record_id
FROM
upd_CLM_INSRBL_INT_insupd
);


-- Component tgt_CLM_INSRBL_INT_ins, Type TARGET 
INSERT INTO DB_T_PROD_CORE.CLM_INSRBL_INT
(
CLM_ID,
INSRBL_INT_ID,
CLM_INSRBL_INT_TYPE_CD,
ATTNY_RPRSNTD_IND,
CLM_INSRBL_INT_DESC_TXT,
VEH_OPRTNL_IND,
ESTMTD_LOSS_AMT,
OWNR_RETAINING_SALV_VEH_IND,
SOLD_OUTSD_SALV_POOL_YD_IND,
SALV_VEH_ABNDN_IND,
OWNR_PRMSSN_IND,
SUBRGTN_POTEN_IND,
ODMTR_READG_VAL,
OCCPY_TYPE_CD,
INJRY_SVRTY_TYPE_CD,
VEH_TOT_LOSS_IND,
CLM_UNIT_NUM,
MEDCR_IND,
PRSN_RLTN_TYPE_CD,
CLM_EXPSR_INCDT_VEH_STLN_IND,
CLM_EXPSR_INCDT_VEH_RCVRD_IND,
VEH_SPEED_MPH_VAL,
VEH_DT_ASGND_TO_SALV,
PRCS_ID,
CLM_INSRBL_INT_STRT_DTTM,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM,
TRANS_END_DTTM
)
SELECT
Exp_clm_insrbl_int_ins.CLM_ID11 as CLM_ID,
Exp_clm_insrbl_int_ins.INSRBL_INT_ID11 as INSRBL_INT_ID,
Exp_clm_insrbl_int_ins.clm_ins_int_typecode1 as CLM_INSRBL_INT_TYPE_CD,
Exp_clm_insrbl_int_ins.TYPECODE_yesno1 as ATTNY_RPRSNTD_IND,
Exp_clm_insrbl_int_ins.Description1 as CLM_INSRBL_INT_DESC_TXT,
Exp_clm_insrbl_int_ins.VehicleOperable1 as VEH_OPRTNL_IND,
Exp_clm_insrbl_int_ins.LossEstimate1 as ESTMTD_LOSS_AMT,
Exp_clm_insrbl_int_ins.OwnerRetainingSalvage1 as OWNR_RETAINING_SALV_VEH_IND,
Exp_clm_insrbl_int_ins.Name1 as SOLD_OUTSD_SALV_POOL_YD_IND,
Exp_clm_insrbl_int_ins.VehicleSalvageAbandoned_alfa1 as SALV_VEH_ABNDN_IND,
Exp_clm_insrbl_int_ins.OwnersPermission1 as OWNR_PRMSSN_IND,
Exp_clm_insrbl_int_ins.SubroPotential_alfa1 as SUBRGTN_POTEN_IND,
Exp_clm_insrbl_int_ins.OdomRead1 as ODMTR_READG_VAL,
Exp_clm_insrbl_int_ins.TGT_REF_TYPE_CD_occ_out1 as OCCPY_TYPE_CD,
Exp_clm_insrbl_int_ins.TGT_REF_TYPE_CD_sev_out1 as INJRY_SVRTY_TYPE_CD,
Exp_clm_insrbl_int_ins.TotalLoss1 as VEH_TOT_LOSS_IND,
Exp_clm_insrbl_int_ins.ID1 as CLM_UNIT_NUM,
Exp_clm_insrbl_int_ins.TYPECODE31 as MEDCR_IND,
Exp_clm_insrbl_int_ins.TGT_IDNTFTN_VAL_personal_relation1 as PRSN_RLTN_TYPE_CD,
Exp_clm_insrbl_int_ins.VehStolenInd1 as CLM_EXPSR_INCDT_VEH_STLN_IND,
Exp_clm_insrbl_int_ins.VehRecoveredInd_alfa1 as CLM_EXPSR_INCDT_VEH_RCVRD_IND,
Exp_clm_insrbl_int_ins.Speed1 as VEH_SPEED_MPH_VAL,
Exp_clm_insrbl_int_ins.DateSalvageAssigned1 as VEH_DT_ASGND_TO_SALV,
Exp_clm_insrbl_int_ins.prcs_id1 as PRCS_ID,
Exp_clm_insrbl_int_ins.in_CLM_INSRBL_INT_STRT_DT1 as CLM_INSRBL_INT_STRT_DTTM,
Exp_clm_insrbl_int_ins.EDW_STRT_DTTM1 as EDW_STRT_DTTM,
Exp_clm_insrbl_int_ins.out_EDW_END_DTTM as EDW_END_DTTM,
Exp_clm_insrbl_int_ins.in_TRANS_STRT_DTTM1 as TRANS_STRT_DTTM,
Exp_clm_insrbl_int_ins.out_TRANS_END_DTTM as TRANS_END_DTTM
FROM
Exp_clm_insrbl_int_ins;


-- Component tgt_CLM_INSRBL_INT_ins, Type Post SQL 
UPDATE  DB_T_PROD_CORE.clm_insrbl_int  FROM 

(SELECT distinct CLM_ID,clm_unit_num, EDW_STRT_DTTM,  TRANS_STRT_DTTM, 

max(EDW_STRT_DTTM) over (partition by clm_unit_num, clm_id ORDER by EDW_STRT_DTTM  ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND'' 

 as lead1,

max(TRANS_STRT_DTTM) over (partition by clm_unit_num, CLM_ID ORDER by  EDW_STRT_DTTM  ASC ,TRANS_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND'' 

 as lead2

FROM DB_T_PROD_CORE.CLM_INSRBL_INT  

WHERE CLM_INSRBL_INT_TYPE_CD=''VEH''

 ) A

set  EDW_END_DTTM=A.lead1,

TRANS_END_DTTM=A.lead2

where  clm_insrbl_int.EDW_STRT_DTTM = A.EDW_STRT_DTTM

and clm_insrbl_int.CLM_ID=A.CLM_ID 

and clm_insrbl_int.CLM_UNIT_NUM=A.CLM_UNIT_NUM 

AND clm_insrbl_int.CLM_INSRBL_INT_TYPE_CD=''VEH''

and lead1 is not null  

and lead2 is not null

and clm_insrbl_int.edw_end_dttm=''9999-12-31 23:59:59.999999''

and clm_insrbl_int.trans_end_dttm=''9999-12-31 23:59:59.999999''

and  clm_insrbl_int.TRANS_STRT_DTTM <> clm_insrbl_int.TRANS_END_DTTM;



UPDATE DB_T_PROD_CORE.CLM_INSRBL_INT  FROM

(SELECT	distinct CLM_ID,INSRBL_INT_ID, EDW_STRT_DTTM ,TRANS_STRT_DTTM,

 MAX(EDW_STRT_DTTM) over (partition by CLM_ID,INSRBL_INT_ID ORDER BY EDW_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND''  as lead1,

 MAX(TRANS_STRT_DTTM) over (partition by CLM_ID,INSRBL_INT_ID ORDER BY EDW_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND''  as lead2

FROM DB_T_PROD_CORE.CLM_INSRBL_INT WHERE CLM_INSRBL_INT_TYPE_CD=''INJ''

) A

set EDW_END_DTTM=A.lead1,

TRANS_END_DTTM=A.lead2

WHERE  clm_insrbl_int.EDW_STRT_DTTM = A.EDW_STRT_DTTM

and clm_insrbl_int.CLM_ID=A.CLM_ID 

and clm_insrbl_int.INSRBL_INT_ID=A.INSRBL_INT_ID 

AND clm_insrbl_int.CLM_INSRBL_INT_TYPE_CD=''INJ''

and lead1 is not null  

and lead2 is not null

and clm_insrbl_int.edw_end_dttm=''9999-12-31 23:59:59.999999''

and clm_insrbl_int.trans_end_dttm=''9999-12-31 23:59:59.999999''

AND clm_insrbl_int.TRANS_STRT_DTTM <> clm_insrbl_int.TRANS_END_DTTM;


-- Component upd_tgt_retired, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_tgt_retired AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_ins_upd_retired.INSRBL_INT_ID1 as INSRBL_INT_ID13,
rtr_ins_upd_retired.CLM_ID1 as CLM_ID13,
rtr_ins_upd_retired.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM3,
NULL as lkp_MEDCR_IND3,
rtr_ins_upd_retired.clm_ins_int_typecode as clm_ins_int_typecode4,
rtr_ins_upd_retired.in_TRANS_STRT_DTTM as in_TRANS_STRT_DTTM4,
rtr_ins_upd_retired.lkp_CLM_INSRBL_INT_STRT_DTTM as LKP_CLM_INSRBL_INT_STRT_DT4,
rtr_ins_upd_retired.DateSalvageAssigned as DateSalvageAssigned4,
1 as UPDATE_STRATEGY_ACTION,
rtr_ins_upd_retired.SOURCE_RECORD_ID
FROM
rtr_ins_upd_retired
);


-- Component EXPTRANS, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXPTRANS AS
(
SELECT
upd_tgt_retired.INSRBL_INT_ID13 as INSRBL_INT_ID13,
upd_tgt_retired.CLM_ID13 as CLM_ID13,
upd_tgt_retired.lkp_MEDCR_IND3 as lkp_MEDCR_IND3,
upd_tgt_retired.lkp_EDW_STRT_DTTM3 as lkp_EDW_STRT_DTTM3,
upd_tgt_retired.clm_ins_int_typecode4 as clm_ins_int_typecode4,
CURRENT_TIMESTAMP as EDW_END_DTTM_exp3,
upd_tgt_retired.in_TRANS_STRT_DTTM4 as o_TRANS_END_DTTM,
upd_tgt_retired.LKP_CLM_INSRBL_INT_STRT_DT4 as LKP_CLM_INSRBL_INT_STRT_DT4,
upd_tgt_retired.source_record_id
FROM
upd_tgt_retired
);


-- Component tgt_CLM_INSRBL_INT_upd_retired, Type TARGET 
MERGE INTO DB_T_PROD_CORE.CLM_INSRBL_INT
USING EXPTRANS ON (CLM_INSRBL_INT.CLM_ID = EXPTRANS.CLM_ID13 AND CLM_INSRBL_INT.INSRBL_INT_ID = EXPTRANS.INSRBL_INT_ID13 AND CLM_INSRBL_INT.CLM_INSRBL_INT_STRT_DTTM = EXPTRANS.LKP_CLM_INSRBL_INT_STRT_DT4 AND CLM_INSRBL_INT.EDW_STRT_DTTM = EXPTRANS.lkp_EDW_STRT_DTTM3)
WHEN MATCHED THEN UPDATE
SET
CLM_ID = EXPTRANS.CLM_ID13,
INSRBL_INT_ID = EXPTRANS.INSRBL_INT_ID13,
CLM_INSRBL_INT_TYPE_CD = EXPTRANS.clm_ins_int_typecode4,
MEDCR_IND = EXPTRANS.lkp_MEDCR_IND3,
CLM_INSRBL_INT_STRT_DTTM = EXPTRANS.LKP_CLM_INSRBL_INT_STRT_DT4,
EDW_STRT_DTTM = EXPTRANS.lkp_EDW_STRT_DTTM3,
EDW_END_DTTM = EXPTRANS.EDW_END_DTTM_exp3,
TRANS_END_DTTM = EXPTRANS.o_TRANS_END_DTTM;


END; ';