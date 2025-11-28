-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_PRTY_ASSET_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
DECLARE
run_id STRING;
 START_DTTM TIMESTAMP;
 END_DTTM TIMESTAMP;
 PRCS_ID VARCHAR;
 p_default_str_cd VARCHAR;
BEGIN 
run_id := (SELECT run_id FROM control_run_id WHERE upper(worklet_name) = upper(:worklet_name) ORDER BY insert_ts DESC LIMIT 1);
 END_DTTM:=   (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''END_DTTM'' ORDER BY insert_ts DESC LIMIT 1);
 START_DTTM:=     (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''START_DTTM'' ORDER BY insert_ts DESC LIMIT 1);
 PRCS_ID:=     (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PRCS_ID'' ORDER BY insert_ts DESC LIMIT 1);
P_DEFAULT_STR_CD :=  (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''P_DEFAULT_STR_CD'' order by insert_ts desc limit 1);


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

             --AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM in ( ''derived'' ,''pcx_holineschcovitemcov_alfa.ChoiceTerm1'', ''cctl_contentlineitemschedule'')

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


-- Component LKP_XREF_PRTY_ASSET, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_XREF_PRTY_ASSET AS
(
SELECT
PRTY_ASSET_ID,
PRTY_ASSET_SBTYPE_CD,
ASSET_HOST_ID_VAL,
PRTY_ASSET_CLASFCN_CD,
SRC_SYS_CD,
LOAD_DTTM
FROM DB_T_PROD_CORE.DIR_PRTY_ASSET
);


-- Component sq_cc_vehicle, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_cc_vehicle AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as Asset_Key,
$2 as Asset_Type,
$3 as Classification_Cd,
$4 as SRC_SYS_CD,
$5 as Asset_Name,
$6 as Asset_Desc,
$7 as Createtime,
$8 as expirationdate,
$9 as reg_num,
$10 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
Select ltrim(rtrim(ID)),assettype,classification_code,src_cd,asset_name,asset_desc,
CreateTime,expirationdate, reg_num											

from (
/** DB_T_CORE_PROD.VEHICLE **/											

/**watercraftmotor**/		
select distinct cast(fixedid_stg  as varchar(100))as id ,											

cast(''PRTY_ASSET_SBTYPE4'' as varchar(50)) as assettype ,											

cast(''PRTY_ASSET_CLASFCN4'' as varchar(50)) as classification_code,''SRC_SYS4'' as src_cd											

,cast('''' as varchar(255)) as asset_name , cast('''' as varchar(255)) as asset_desc,cast(null as varchar(100)) as reg_num											

, pcx_pawatercraftmotor_alfa.createtime_stg as CreateTime, 											

case when pcx_pawatercraftmotor_alfa.expirationdate_stg is null then cast(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6))   else pcx_pawatercraftmotor_alfa.expirationdate_stg end as expirationdate											

from DB_T_PROD_STAG.pcx_pawatercraftmotor_alfa 											

where fixedid_stg is not null 											

and pcx_pawatercraftmotor_alfa.UpdateTime_stg > (:START_DTTM) 											

and pcx_pawatercraftmotor_alfa.UpdateTime_stg <= (:END_DTTM	)										
Union											

/**WaterCrafttrailer**/											

select distinct cast(fixedid_stg  as varchar(100))as id , ''PRTY_ASSET_SBTYPE4''  as assettype ,''PRTY_ASSET_CLASFCN5'' as classification_code,''SRC_SYS4'' as src_cd											

,'''' as asset_name , '''' as asset_desc,cast(null as varchar(100)) as reg_num											

, pcx_pawatercrafttrailer_alfa.createtime_stg,											

case when pcx_pawatercrafttrailer_alfa.expirationdate_stg is null then cast(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6))  else pcx_pawatercrafttrailer_alfa.expirationdate_stg end as expirationdate											

from DB_T_PROD_STAG.pcx_pawatercrafttrailer_alfa 											

where fixedid_stg is not null											

and pcx_pawatercrafttrailer_alfa.UpdateTime_stg>(:START_DTTM)											

and pcx_pawatercrafttrailer_alfa.UpdateTime_stg <= (:END_DTTM)											

											

											

Union											

/**Motor Vehicle**/											

select distinct cast(fixedid_stg  as varchar(100))as id 											

, ''PRTY_ASSET_SBTYPE4''  as assettype ,											

''PRTY_ASSET_CLASFCN3'' as classification_code,''SRC_SYS4'' as src_cd											

,'''' as asset_name , '''' as asset_desc,cast(null as varchar(100)) as reg_num											

,pc_personalvehicle.createtime_stg as createtime, 											

case when pc_personalvehicle.expirationdate_stg is null then cast(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6))  											

else pc_personalvehicle.expirationdate_stg end as expirationdate											

FROM 											

(Select fixedid_stg,createtime_stg,expirationdate_stg 
from DB_T_PROD_STAG.pc_personalvehicle											

where 											

 pc_personalvehicle.UpdateTime_stg> (:START_DTTM)											

    and pc_personalvehicle.UpdateTime_stg <= (:END_DTTM)											

    and  (pc_personalvehicle.ExpirationDate_stg is null 											

    or pc_personalvehicle.ExpirationDate_stg >(:START_DTTM)	)										

) pc_personalvehicle											

    Where pc_personalvehicle.fixedid_stg is not null 											

											

Union											

/**Motor DB_T_CORE_PROD.VEHICLE Third party/Unverified**/											

select distinct 											

cast(case 											

when (cc_vehicle.PolicySystemId_stg is null and cc_vehicle.Vin_stg is not null) then  ''VIN:''|| cc_vehicle.vin_stg 											

when (cc_vehicle.PolicySystemId_stg is null and cc_vehicle.Vin_stg is null and cc_vehicle.LicensePlate_stg is not null) then ''LP:'' || cc_vehicle.licenseplate_stg											

when (cc_vehicle.PolicySystemId_stg is null and cc_vehicle.Vin_stg is null and cc_vehicle.LicensePlate_stg is null) then cc_vehicle.PublicID_stg											

end as varchar(100))as id 											

,''PRTY_ASSET_SBTYPE4''  as assettype ,''PRTY_ASSET_CLASFCN3'' as classification_code,''SRC_SYS6'' as src_cd											

,cctl_incident.name_stg as asset_name , 											

cc_incident.Description_stg as asset_desc,cast(null as varchar(100)) as reg_num											

, cc_vehicle.createtime_stg, 											

cast(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6)) as expirationdate											

from 											

(Select cc_vehicle.*,NULL as Subtype_stg from db_t_prod_stag.cc_vehicle) cc_vehicle 											

left outer join  DB_T_PROD_STAG.cc_incident 											

    on  cc_vehicle.id_stg =cc_incident.vehicleid_stg											

left outer join DB_T_PROD_STAG.cctl_incident on cc_vehicle.Subtype_stg = cctl_incident.id_stg											

where   											

cc_vehicle.UpdateTime_stg > (:START_DTTM)											

    and cc_vehicle.UpdateTime_stg <= (:END_DTTM)											

and  policysystemid_stg is null											

											

UNION											

         /** Real Estate **/											

/** Main Dwelling  **/											

											

select distinct cast(pcx_dwelling_hoe.fixedid_stg  as varchar(100))as id,											

''PRTY_ASSET_SBTYPE5'' as assettype ,''PRTY_ASSET_CLASFCN1'' as classification_code,''SRC_SYS4'' as src_cd											

,'''' as asset_name , '''' as asset_desc,cast(null as varchar(100)) as reg_num											

,  pcx_dwelling_hoe.createtime_stg,  											

case when pcx_dwelling_hoe.expirationdate_stg is null then cast(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6))  											

else pcx_dwelling_hoe.expirationdate_stg end as expirationdate											

from 											

(											

Select distinct  pcx_dwelling_hoe.fixedid_stg,pcx_dwelling_hoe.createtime_stg,											

case when pcx_dwelling_hoe.ExpirationDate_stg is null then pc_policyperiod.PeriodEnd_stg when pcx_dwelling_hoe.ExpirationDate_stg is not null then pcx_dwelling_hoe.ExpirationDate_stg											

end as ExpirationDate_stg 											

from DB_T_PROD_STAG.pcx_dwelling_hoe 											

left outer join DB_T_PROD_STAG.pc_policyperiod on pc_policyperiod.id_stg = pcx_dwelling_hoe.branchid_stg											

left outer join DB_T_PROD_STAG.pc_policy on pc_policy.ID_stg=pc_policyperiod.PolicyID_stg											

left outer join (											

 select pcx_dwelling_hoe.FixedID_stg as homealerfixedid_stg, pcx_dwelling_hoe.branchid_stg as branchid_stg, HomeAlertCode_stg as homealert_cd, HurrMitigationCreditAmt_stg 											

from   DB_T_PROD_STAG.pc_policyperiod											

inner join DB_T_PROD_STAG.pcx_dwelling_hoe on pcx_dwelling_hoe.BranchID_stg=pc_policyperiod.ID_stg											

inner join DB_T_PROD_STAG.pcx_dwellingratingfactor_alfa on pcx_dwelling_hoe.FixedID_stg  = pcx_dwellingratingfactor_alfa.Dwelling_HOE_stg 											

and pcx_dwellingratingfactor_alfa.BranchID_stg=pc_policyperiod.ID_stg											

where pcx_dwelling_hoe.ExpirationDate_stg is null and pcx_dwellingratingfactor_alfa.ExpirationDate_stg is null 											

) homealert on pcx_dwelling_hoe.FixedID_stg=homealert.homealerfixedid_stg and pcx_dwelling_hoe.branchid_stg=homealert.branchid_stg											

											

join DB_T_PROD_STAG.pcx_holocation_hoe on pcx_dwelling_hoe.holocation_stg = pcx_holocation_hoe.ID_stg											

where pcx_dwelling_hoe.ExpirationDate_stg is null											

and											

pcx_dwelling_hoe.UpdateTime_stg>(:START_DTTM) AND  pcx_dwelling_hoe.UpdateTime_stg <= (:END_DTTM)											

) pcx_dwelling_hoe											

where  pcx_dwelling_hoe.fixedid_stg is not null											

											

											

union											

/**Dwelling Personal Property and Other Structure**/											

select 											

distinct											

cast(pcx_holineschedcovitem_alfa.FixedID_stg as varchar(100))as id,											

case when (pc_etlclausepattern.PatternID_stg like ''%ScheduledProperty%'' or pc_etlclausepattern.PatternID_stg like ''HOLI%'') then ''PRTY_ASSET_SBTYPE7''    																

when (pc_etlclausepattern.PatternID_stg like ''%SpecificOtherStructure%'' or pc_etlclausepattern.PatternID_stg like ''HODW%'' or pc_etlclausepattern.PatternID_stg like ''HOSI%'') 																

then ''PRTY_ASSET_SBTYPE5'' end as assettype																

,case when assettype=''PRTY_ASSET_SBTYPE5'' THEN ''PRTY_ASSET_CLASFCN1'' else ChoiceTerm1_stg end as classification_code,											

''SRC_SYS4'' as src_cd											

,'''' as asset_name											

,ChoiceTerm1_stg as dsc,cast(null as varchar(100)) as reg_num											

, pcx_holineschcovitemcov_alfa.createtime_stg											

,case when pcx_holineschcovitemcov_alfa.expirationdate_stg is null then cast(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6))   											

else pcx_holineschcovitemcov_alfa.expirationdate_stg end as expirationdate											

from 											

DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa											

left outer join DB_T_PROD_STAG.pc_etlclausepattern 											

    on pc_etlclausepattern.PatternID_stg=pcx_holineschcovitemcov_alfa.PatternCode_stg											

left outer join DB_T_PROD_STAG.pcx_holineschedcovitem_alfa 											

    on pcx_holineschedcovitem_alfa.ID_stg=pcx_holineschcovitemcov_alfa.HOLineSchCovItem_stg											

where											

	 pcx_holineschedcovitem_alfa.FixedID_stg is not null 											

and pcx_holineschcovitemcov_alfa.UpdateTime_stg > (:START_DTTM) 											

and pcx_holineschcovitemcov_alfa.UpdateTime_stg <= (:END_DTTM)											

											

											

UNION											

/** BP7 Building and property **/											

											

SELECT distinct											

    cast(fixedid_stg as varchar(100)) as id, 											

    cast(''PRTY_ASSET_SBTYPE13'' as varchar(50)) as assettype,											

    cast(classificationcode_stg as varchar(50))as classification_code,											

    ''SRC_SYS4'' as src_cd,											

    '''' as asset_name, 											

Building_desc as asset_desc ,/*  Changes to asset decription Column											 */
    cast(null as varchar(100)) as reg_num,											

    createtime_stg, 											

    coalesce(expirationdate_stg, cast(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6))) as expirationdate											

FROM 											

(SELECT DISTINCT c.CreateTime_stg,c.FixedID_stg,c.ExpirationDate_stg,cp.TYPECODE_stg as classificationcode_stg, building.description_stg as Building_desc											

FROM DB_T_PROD_STAG.pcx_bp7classification c											

INNER JOIN (select b.*, rank() over (partition by b.FixedId_stg order by b.UpdateTime_stg desc) r from DB_T_PROD_STAG.pcx_bp7building b) b 											

                                on c.Building_stg = b.FixedID_stg											

                                and c.branchid_stg=b.branchid_stg											

                                and b.r = 1 											

/** EIM-15651 INCLUDED DB_T_PROD_STAG.PC_BUILDING table to have Building description column ****/               											

INNER JOIN (select description_stg,id_stg from db_t_prod_stag.pc_building) building  on building.id_stg = b.Building_stg 											

INNER JOIN (select TYPECODE_stg,ID_stg from db_t_prod_stag.pctl_bp7classificationproperty) cp on cp.ID_stg = c.bp7classpropertytype_stg 											

INNER JOIN  DB_T_PROD_STAG.pctl_bp7classdescription cdesc on c.bp7classdescription_stg = cdesc.ID_stg 											

INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg = b.BranchID_stg											

INNER JOIN DB_T_PROD_STAG.pc_policy p on p.id_stg = pp.PolicyID_stg 											

INNER JOIN (select l.*, rank() over (partition by l.FixedId_stg order by l.UpdateTime_stg desc) r from DB_T_PROD_STAG.pcx_bp7location l) l 											

                                on b.Location_stg = l.FixedID_stg											

                                and l.r = 1											

INNER JOIN DB_T_PROD_STAG.pc_policyline pol on pol.BranchID_stg = pp.ID_stg											

WHERE b.ExpirationDate_stg IS NULL											

AND c.ExpirationDate_stg IS NULL											

AND l.ExpirationDate_stg IS NULL											

AND ((c.UPDATETIME_stg > (:START_DTTM) AND c.UPDATETIME_stg <= (:END_DTTM))											

    OR (b.UPDATETIME_stg > (:START_DTTM) AND b.UPDATETIME_stg <= (:END_DTTM))											

                OR (l.UPDATETIME_stg > (:START_DTTM) AND l.UPDATETIME_stg <= (:END_DTTM)))											

                ) pcx_bp7building											

where fixedid_stg is not null											

											

/*UNION											

											

SELECT distinct											

    cast(fixedid_stg as varchar(100)) as id, 											

    cast(''PRTY_ASSET_SBTYPE14'' as varchar(50)) as assettype,											

    StringTerm1_stg as classification_code,											

    ''SRC_SYS4'' as src_cd,											

    '''' as asset_name, 											

    '''' as asset_desc, cast(null as varchar(100)) as reg_num,											

    createtime_stg, 											

    coalesce(expirationdate_stg, cast(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6))) as expirationdate											

FROM (SELECT bs.FixedID_stg,bs.CreateTime_stg,bs.StringTerm1_stg,bs.ExpirationDate_stg											

FROM DB_T_PROD_STAG.pcx_bp7bldgschedcovitemcov bs											

WHERE bs.UpdateTime_stg > (START_DTTM) and bs.UpdateTime_stg <= (End_Dttm)											

) pcx_bp7bldgschedcovitemcov											

where fixedid_stg is not null*/ /* - Commenting out as there is no mapping and worklet in DB_T_PROD_CORE.prod environment .only stage table exists. 											 */
											

UNION											

											

select DISTINCT											

cast(fixedid_stg as varchar(100)) as id, 											

cast(''PRTY_ASSET_SBTYPE29'' AS varchar(50)) as PRTY_ASSET_SBTYPE_CD,											

cast(''PRTY_ASSET_CLASFCN8'' as varchar(50))as classification_code,											

''SRC_SYS4'' as src_cd,											

'''' as asset_name											

,ltrim(COALESCE(StringCol1_stg,'''')||'' ''||COALESCE(StringCol2_stg,'''')||'' ''||COALESCE(StringCol3_stg,'''')||'' ''||COALESCE(StringCol4_stg,'''')||'' ''||COALESCE(longStringCol1_stg,'''')||'' ''||COALESCE(TypeKeyCol1_stg,'''')||'' ''||COALESCE(TypeKeyCol2_stg,'''')) as ASSET_DESC2,											

cast(null as varchar(100)) as reg_num,											

pcx_bp7lineschedcovitem.createtime_stg, 											

coalesce((expirationdate_stg), cast(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6))) as expirationdate											

from    											

DB_T_PROD_STAG.pcx_bp7lineschedcovitem join DB_T_PROD_STAG.pc_policyperiod 											

    on pcx_bp7lineschedcovitem.BranchID_stg=pc_policyperiod.ID_stg											

where   pcx_bp7lineschedcovitem.UpdateTime_stg > (:START_DTTM) 											

    and pcx_bp7lineschedcovitem.UpdateTime_stg<= (:END_DTTM)											

and ExpirationDate_stg is null  											

											

											

											

UNION											

											

Select DISTINCT											

cast(fixedid_stg as varchar(100)) as id, 											

cast(''PRTY_ASSET_SBTYPE31'' AS varchar(50)) as PRTY_ASSET_SBTYPE_CD,											

cast(''PRTY_ASSET_CLASFCN8''AS varchar(50)) as classification_code,											

''SRC_SYS4'' as src_cd,											

'''' as asset_name											

,ltrim(COALESCE(StringCol1_stg,'''')||'' ''||COALESCE(StringCol2_stg,'''')||'' ''||COALESCE(StringCol3_stg,'''')||'' ''||COALESCE(StringCol4_stg,'''')||'' ''||COALESCE(longStringCol1_stg,'''')||'' ''||COALESCE(TypeKeyCol1_stg,'''')||'' ''||COALESCE(TypeKeyCol2_stg,'''')) as ASSET_DESC2,											

cast(null as varchar(100)) as reg_num,											

pcx_bp7lineschedexclitem.createtime_stg, 											

coalesce((expirationdate_stg), cast(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6))) as expirationdate											

from 											

 DB_T_PROD_STAG.pcx_bp7lineschedexclitem  join DB_T_PROD_STAG.pc_policyperiod 											

    on pcx_bp7lineschedexclitem.BranchID_stg=pc_policyperiod.ID_stg											

where   pcx_bp7lineschedexclitem.UpdateTime_stg > (:START_DTTM) 											

    and pcx_bp7lineschedexclitem.UpdateTime_stg<= (:END_DTTM)											

and ExpirationDate_stg is null											

											

											

UNION											

											

select DISTINCT											

cast(fixedid_stg as varchar(100)) as id, 											

cast(''PRTY_ASSET_SBTYPE30'' AS varchar(50)) as PRTY_ASSET_SBTYPE_CD,											

cast(''PRTY_ASSET_CLASFCN8'' as varchar(50)) as classification_code,											

''SRC_SYS4'' as src_cd,											

'''' as asset_name											

,ltrim(COALESCE(StringCol1_stg,'''')||'' ''||COALESCE(StringCol2_stg,'''')||'' ''||COALESCE(StringCol3_stg,'''')||'' ''||COALESCE(StringCol4_stg,'''')||'' ''||COALESCE(longStringCol1_stg,'''')||'' ''||COALESCE(TypeKeyCol1_stg,'''')||'' ''||COALESCE(TypeKeyCol2_stg,'''')) as ASSET_DESC2,											

cast(null as varchar(100)) as reg_num,											

pcx_bp7lineschedconditem.createtime_stg, 											

coalesce((expirationdate_stg), 											

cast(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6))) as expirationdate											

FROM											

DB_T_PROD_STAG.pcx_bp7lineschedconditem  join DB_T_PROD_STAG.pc_policyperiod 											

    on pcx_bp7lineschedconditem.BranchID_stg=pc_policyperiod.ID_stg											

where   pcx_bp7lineschedconditem.UpdateTime_stg > (:START_DTTM) 											

    and pcx_bp7lineschedconditem.UpdateTime_stg<= (:END_DTTM)											

and ExpirationDate_stg is null 											

											

UNION											

											

select DISTINCT											

cast(fixedid_stg as varchar(100)) as id, 											

cast(''PRTY_ASSET_SBTYPE20'' AS varchar(50)) as PRTY_ASSET_SBTYPE_CD,											

cast(''PRTY_ASSET_CLASFCN8'' as varchar(50)) as classification_code,											

''SRC_SYS4'' as src_cd,											

'''' as asset_name											

,ltrim(COALESCE(StringCol1_stg,'''')||'' ''||COALESCE(StringCol2_stg,'''')||'' ''||COALESCE(StringCol3_stg,'''')||'' ''||COALESCE(StringCol4_stg,'''')||'' ''||COALESCE(longStringCol1_stg,'''')||'' ''||COALESCE(TypeKeyCol1_stg,'''')||'' ''||COALESCE(TypeKeyCol2_stg,'''')) as ASSET_DESC2,											

cast(null as varchar(100)) as reg_num,											

pcx_BP7LocSchedCovItem.createtime_stg, 											

coalesce((expirationdate_stg), cast(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6))) as expirationdate											

from    DB_T_PROD_STAG.pcx_BP7LocSchedCovItem    											

 join DB_T_PROD_STAG.pc_policyperiod 											

    on pcx_bp7locschedcovitem.BranchID_stg=pc_policyperiod.ID_stg											

where   pcx_bp7locschedcovitem.UpdateTime_stg > (:START_DTTM) 											

    and pcx_bp7locschedcovitem.UpdateTime_stg <= (:END_DTTM)											

    and ExpirationDate_stg is null											

											

											

UNION											

											

select DISTINCT											

cast(fixedid_stg as varchar(100)) as id, 											

cast(''PRTY_ASSET_SBTYPE22'' AS varchar(50)) as PRTY_ASSET_SBTYPE_CD,											

cast(''PRTY_ASSET_CLASFCN8'' as varchar(50)) as classification_code,											

''SRC_SYS4'' as src_cd,											

'''' as asset_name											

,ltrim(COALESCE(StringCol1_stg,'''')||'' ''||COALESCE(StringCol2_stg,'''')||'' ''||COALESCE(StringCol3_stg,'''')||'' ''||COALESCE(StringCol4_stg,'''')||'' ''||COALESCE(longStringCol1_stg,'''')||'' ''||COALESCE(TypeKeyCol1_stg,'''')||'' ''||COALESCE(TypeKeyCol2_stg,'''')) as ASSET_DESC2,											

cast(null as varchar(100)) as reg_num,											

pcx_BP7LocSchedexclItem.createtime_stg, 											

coalesce((expirationdate_stg), cast(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6))) as expirationdate											

from    DB_T_PROD_STAG.pcx_BP7LocSchedexclItem  											

join DB_T_PROD_STAG.pc_policyperiod 											

    on pcx_bp7locschedexclitem.BranchID_stg=pc_policyperiod.ID_stg											

where   pcx_bp7locschedexclitem.UpdateTime_stg > (:START_DTTM) 											

    and pcx_bp7locschedexclitem.UpdateTime_stg  <= (:END_DTTM)											

    and ExpirationDate_stg is null 											

											

											

UNION											

											

select DISTINCT											

cast(fixedid_stg as varchar(100)) as id, 											

 cast(''PRTY_ASSET_SBTYPE21'' AS varchar(50))as PRTY_ASSET_SBTYPE_CD,											

cast(''PRTY_ASSET_CLASFCN8''  as varchar(50)) as classification_code,											

''SRC_SYS4'' as src_cd,											

'''' as asset_name											

,ltrim(COALESCE(StringCol1_stg,'''')||'' ''||COALESCE(StringCol2_stg,'''')||'' ''||COALESCE(StringCol3_stg,'''')||'' ''||COALESCE(StringCol4_stg,'''')||'' ''||COALESCE(longStringCol1_stg,'''')||'' ''||COALESCE(TypeKeyCol1_stg,'''')||'' ''||COALESCE(TypeKeyCol2_stg,'''')) as ASSET_DESC2,											

cast(null as varchar(100)) as reg_num,											

pcx_BP7LocSchedCondItem.createtime_stg, 											

    coalesce((expirationdate_stg), cast(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6))) as expirationdate											

from    DB_T_PROD_STAG.pcx_BP7LocSchedCondItem 											

join DB_T_PROD_STAG.pc_policyperiod on pcx_bp7locschedconditem.BranchID_stg=pc_policyperiod.ID_stg 											

where  pcx_bp7locschedconditem.UpdateTime_stg > (:START_DTTM) 											

and pcx_bp7locschedconditem.UpdateTime_stg <= (:END_DTTM)											

and ExpirationDate_stg is null 											

 											

											

UNION											

											

select DISTINCT											

cast(bp7item.fixedid_stg as varchar(100)) as id, 											

cast(''PRTY_ASSET_SBTYPE23'' as varchar(50))AS PRTY_ASSET_SBTYPE_CD,											

cast(''PRTY_ASSET_CLASFCN8'' as varchar(50)) as classification_code,											

''SRC_SYS4'' as src_cd,											

'''' as asset_name											

/* ,ltrim(COALESCE(StringCol1_stg,'''')||'' ''||COALESCE(StringCol2_stg,'''')||'' ''||COALESCE(StringCol3_stg,'''')||'' ''||COALESCE(StringCol4_stg,'''')||'' ''||COALESCE(longStringCol1_stg,'''')||'' ''||COALESCE(TypeKeyCol1_stg,'''')||'' ''||COALESCE(TypeKeyCol2_stg,'''')) as ASSET_DESC2,											 */
,bp7cov.StringTerm1_stg as asset_desc /* Commented the above logic and added this column as part of EIM-19923											 */
,cast(null as varchar(100)) as reg_num,											

bp7item.createtime_stg, 											

    coalesce((bp7item.expirationdate_stg), cast(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6))) as expirationdate											

from       											

    DB_T_PROD_STAG.pcx_BP7BldgSchedCovItem bp7item 											

            left join (select distinct BldgSchedCovItem_stg,StringTerm1_stg from DB_T_PROD_STAG.pcx_bp7bldgschedcovitemcov 											

        qualify row_number() over (partition by BldgSchedCovItem_stg order by updatetime_stg desc)=1											

        )bp7cov											

            on bp7item.ID_stg = bp7cov.BldgSchedCovItem_stg											

    join DB_T_PROD_STAG.pc_policyperiod 											

    on  bp7item.BranchID_stg=pc_policyperiod.ID_stg											

where    bp7item.UpdateTime_stg  > (:START_DTTM) 											

    and  bp7item.UpdateTime_stg <= (:END_DTTM)											

    and  bp7item.ExpirationDate_stg is null 											

											

											

UNION											

											

select DISTINCT											

cast(fixedid_stg as varchar(100)) as id, 											

cast(''PRTY_ASSET_SBTYPE25'' AS varchar(50)) as PRTY_ASSET_SBTYPE_CD,											

cast(''PRTY_ASSET_CLASFCN8'' as varchar(50)) as classification_code,											

''SRC_SYS4'' as src_cd,											

'''' as asset_name											

,ltrim(COALESCE(StringCol1_stg,'''')||'' ''||COALESCE(StringCol2_stg,'''')||'' ''||COALESCE(StringCol3_stg,'''')||'' ''||COALESCE(StringCol4_stg,'''')||'' ''||COALESCE(longStringCol1_stg,'''')||'' ''||COALESCE(TypeKeyCol1_stg,'''')||'' ''||COALESCE(TypeKeyCol2_stg,'''')) as ASSET_DESC2,											

cast(null as varchar(100)) as reg_num,											

pcx_BP7BldgSchedexclItem.createtime_stg,											

coalesce((expirationdate_stg), cast(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6))) as expirationdate											

from    DB_T_PROD_STAG.pcx_BP7BldgSchedexclItem  											

 join DB_T_PROD_STAG.pc_policyperiod 											

    on pcx_bp7bldgschedexclitem.BranchID_stg=pc_policyperiod.ID_stg											

where   ExpirationDate_stg is null 											

and pcx_bp7bldgschedexclitem.UpdateTime_stg > (:START_DTTM) 											

    and pcx_bp7bldgschedexclitem.UpdateTime_stg<= (:END_DTTM)											

											

											

UNION											

											

											

select DISTINCT											

cast(fixedid_stg as varchar(100)) as id, 											

cast(''PRTY_ASSET_SBTYPE24''  as varchar(50))AS PRTY_ASSET_SBTYPE_CD,											

cast(''PRTY_ASSET_CLASFCN8'' as varchar(50)) as classification_code,											

''SRC_SYS4'' as src_cd,											

'''' as asset_name											

,ltrim(COALESCE(StringCol1_stg,'''')||'' ''||COALESCE(StringCol2_stg,'''')||'' ''||COALESCE(StringCol3_stg,'''')||'' ''||COALESCE(StringCol4_stg,'''')||'' ''||COALESCE(longStringCol1_stg,'''')||'' ''||COALESCE(TypeKeyCol1_stg,'''')||'' ''||COALESCE(TypeKeyCol2_stg,'''')) as ASSET_DESC2,											

cast(null as varchar(100)) as reg_num,											

pcx_bp7bldgschedconditem.createtime_stg, 											

coalesce((expirationdate_stg), cast(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6))) as expirationdate											

from 											

DB_T_PROD_STAG.pcx_bp7bldgschedconditem join DB_T_PROD_STAG.pc_policyperiod 											

    on  pcx_bp7bldgschedconditem.BranchID_stg=pc_policyperiod.ID_stg											

where   pcx_bp7bldgschedconditem.UpdateTime_stg  > (:START_DTTM) 											

    and  pcx_bp7bldgschedconditem.UpdateTime_stg <= (:END_DTTM)											

    and ExpirationDate_stg is null 											

											

											

UNION											

											

select DISTINCT											

cast(fixedid_stg as varchar(100)) as id, 											

cast(''PRTY_ASSET_SBTYPE26'' AS varchar(50)) as PRTY_ASSET_SBTYPE_CD,											

cast(''PRTY_ASSET_CLASFCN8'' as varchar(50)) as classification_code,											

''SRC_SYS4'' as src_cd,											

'''' as asset_name											

,ltrim(COALESCE(StringCol1_stg,'''')||'' ''||COALESCE(StringCol2_stg,'''')||'' ''||COALESCE(StringCol3_stg,'''')||'' ''||COALESCE(StringCol4_stg,'''')||'' ''||COALESCE(longStringCol1_stg,'''')||'' ''||COALESCE(TypeKeyCol1_stg,'''')||'' ''||COALESCE(TypeKeyCol2_stg,'''')) as ASSET_DESC2,											

cast(StringCol4_stg as varchar(100)) as reg_num,											

pcx_BP7ClassSchedCovItem.createtime_stg, 											

coalesce((expirationdate_stg), cast(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6))) as expirationdate											

FROM 											

DB_T_PROD_STAG.pcx_BP7ClassSchedCovItem    											

    join DB_T_PROD_STAG.pc_policyperiod 											

    on pcx_bp7classschedcovitem.BranchID_stg= pc_policyperiod.ID_stg											

where    pcx_bp7classschedcovitem.UpdateTime_stg > (:START_DTTM) 											

    and  pcx_bp7classschedcovitem.UpdateTime_stg<= (:END_DTTM)											

    and ExpirationDate_stg is null 											

											

											

UNION											

											

Select DISTINCT											

cast(fixedid_stg as varchar(100)) as id, 											

CAST(''PRTY_ASSET_SBTYPE28'' AS VARCHAR(50)) as PRTY_ASSET_SBTYPE_CD,											

CAST(''PRTY_ASSET_CLASFCN8'' AS VARCHAR(50)) as classification_code,											

''SRC_SYS4'' as src_cd,											

'''' as asset_name											

,ltrim(COALESCE(StringCol1_stg,'''')||'' ''||COALESCE(StringCol2_stg,'''')||'' ''||COALESCE(StringCol3_stg,'''')||'' ''||COALESCE(StringCol4_stg,'''')||'' ''||COALESCE(longStringCol1_stg,'''')||'' ''||COALESCE(TypeKeyCol1_stg,'''')||'' ''||COALESCE(TypeKeyCol2_stg,'''')) as ASSET_DESC2,											

cast(StringCol4_stg as varchar(100)) as reg_num,											

pcx_BP7ClassSchedexclItem.createtime_stg, 											

coalesce((expirationdate_stg), cast(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6))) as expirationdate											

FROM											

DB_T_PROD_STAG.pcx_BP7ClassSchedexclItem    											

join DB_T_PROD_STAG.pc_policyperiod 											

    on pcx_bp7classschedexclitem.BranchID_stg=pc_policyperiod.ID_stg											

where   pcx_bp7classschedexclitem.UpdateTime_stg > (:START_DTTM) 											

    and pcx_bp7classschedexclitem.UpdateTime_stg <= (:END_DTTM)											

    and ExpirationDate_stg is null 											

											

											

UNION											

											

select DISTINCT											

cast(fixedid_stg as varchar(100)) as id, 											

CAST(''PRTY_ASSET_SBTYPE27'' AS VARCHAR(50)) AS PRTY_ASSET_SBTYPE_CD,											

CAST(''PRTY_ASSET_CLASFCN8'' AS VARCHAR(50)) AS classification_code,											

''SRC_SYS4'' as src_cd,											

'''' as asset_name											

,ltrim(COALESCE(StringCol1_stg,'''')||'' ''||COALESCE(StringCol2_stg,'''')||'' ''||COALESCE(StringCol3_Stg,'''')||'' ''||COALESCE(StringCol4_stg,'''')||'' ''||COALESCE(longStringCol1_stg,'''')||'' ''||COALESCE(TypeKeyCol1_stg,'''')||'' ''||COALESCE(TypeKeyCol2_stg,'''')) as ASSET_DESC2,											

cast(StringCol4_stg as varchar(100)) as reg_num,											

pcx_BP7ClassSchedCondItem.createtime_stg, 											

coalesce((expirationdate_stg), cast(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6))) as expirationdate											

from 											

DB_T_PROD_STAG.pcx_BP7ClassSchedCondItem 											

join DB_T_PROD_STAG.pc_policyperiod 											

    on pcx_bp7classschedconditem.BranchID_stg=pc_policyperiod.ID_stg											

where   pcx_bp7classschedconditem.UpdateTime_stg > (:START_DTTM) 											

    and pcx_bp7classschedconditem.UpdateTime_stg<= (:START_DTTM)											

and ExpirationDate_stg is null 											

											

											

         /** CLAIM PROPERTY AND OTHER **/											

UNION											

/** DWELLING **/											

/* DwellingIncident: Unverfied dwelling											 */
/* OtherStructureIncident: Unverfied/liability other structure such as pool, garage etc											 */
/* PropertyContentsIncident: Unverfied/liability property content											 */
/* FixedPropertyIncident: liability dwelling											 */
select distinct											

cast(cc_incident.PublicID_stg as varchar(100)) as id, 											

case when cctl_incident.name_stg = ''FixedPropertyIncident'' then ''PRTY_ASSET_SBTYPE5''											

when cctl_incident.name_stg =''OtherStructureIncident'' then ''PRTY_ASSET_SBTYPE11'' end as assettype , 											

case when cctl_incident.name_stg =''FixedPropertyIncident'' then ''PRTY_ASSET_CLASFCN1''											

when cctl_incident.name_stg = ''OtherStructureIncident'' then ''PRTY_ASSET_CLASFCN7'' end as classification_type ,											

''SRC_SYS6'' as src_cd,											

cctl_incident.name_stg as asset_name ,											

cc_incident.Description_stg as asset_desc,cast(null as varchar(100)) reg_num											

, cc_incident.createtime_stg, cast(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6)) as expirationdate											

from 											

DB_T_PROD_STAG.cc_incident 											

inner join (											

    select  cc_claim.* 											

    from    DB_T_PROD_STAG.cc_claim 											

    inner join DB_T_PROD_STAG.cctl_claimstate 											

        on cc_claim.State_stg= cctl_claimstate.id_stg 											

    where   cctl_claimstate.name_stg <> ''Draft'') cc_claim 											

    on cc_claim.id_stg=cc_incident.claimid_stg											

inner join DB_T_PROD_STAG.cctl_incident on cc_incident.Subtype_stg = cctl_incident.id_stg  											

WHERE   											

cc_incident.UpdateTime_stg > (:START_DTTM)											

and cc_incident.UpdateTime_stg <= (:END_DTTM)											

and cctl_incident.name_stg in (''FixedPropertyIncident'',''OtherStructureIncident'')											

											

UNION											

											

select distinct											

cast(cc_incident.PublicID_stg as varchar(100)) as id, 											

''PRTY_ASSET_SBTYPE5'' as assettype , 											

''PRTY_ASSET_CLASFCN1'' as classification_type , ''SRC_SYS6'' as src_cd,											

cctl_incident.name_stg as asset_name , 											

cc_incident.Description_stg as asset_desc,											

cast(null as varchar(100)) reg_num,											

cc_incident.createtime_stg, cast(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6))  as expirationdate											

from 											

DB_T_PROD_STAG.cc_incident 											

inner join (											

    select  cc_claim.* 											

    from    DB_T_PROD_STAG.cc_claim 											

    inner join DB_T_PROD_STAG.cctl_claimstate 											

        on cc_claim.State_stg= cctl_claimstate.id_stg 											

    where   cctl_claimstate.name_stg <> ''Draft'') cc_claim 											

    on cc_claim.id_stg=cc_incident.claimid_stg 											

    left outer join DB_T_PROD_STAG.cc_address 											

              on cc_claim.LossLocationID_stg = cc_address.ID_stg											

left outer join DB_T_PROD_STAG.cc_policylocation 											

              on cc_policylocation.AddressID_stg= cc_address.id_stg											

inner join DB_T_PROD_STAG.cctl_incident on cc_incident.Subtype_stg = cctl_incident.id_stg											

where 											

cc_incident.UpdateTime_stg > (:START_DTTM)											

and cc_incident.UpdateTime_stg <= (:END_DTTM)											

and cctl_incident.name_stg=''DwellingIncident''											

and cc_policylocation.PolicySystemId_stg is null											

											

UNION											

											

SELECT 											

distinct 											

cast(cc_assessmentcontentitem.PublicID_stg as varchar(100)) as id, 											

''PRTY_ASSET_SBTYPE11'' as assettype , 											

cctl_contentlineitemschedule.TYPECODE_stg as Typecode_contentlineitemschedule, 											

''SRC_SYS6'' as src_cd,											

cctl_contentlineitemcategory.TYPECODE_stg as Typecode_contentlineitemcategory,											

 cc_assessmentcontentitem.Description_stg as asset_desc,											

 cast(null as varchar(100)) reg_num											

,cc_assessmentcontentitem.createtime_stg,											

cast(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6))  as expirationdate											

FROM											

DB_T_PROD_STAG.cc_incident 											

inner join DB_T_PROD_STAG.cctl_incident 											

    on cc_incident.Subtype_stg = cctl_incident.id_stg											

inner join											

(											

    select  cc_claim.* 											

    from    DB_T_PROD_STAG.cc_claim 											

    inner join DB_T_PROD_STAG.cctl_claimstate 											

        on cc_claim.State_stg= cctl_claimstate.id_stg 											

    where   cctl_claimstate.name_stg <> ''Draft'') cc_claim 											

    on cc_claim.id_stg=cc_incident.ClaimID_stg											

inner join DB_T_PROD_STAG.cc_assessmentcontentitem 											

    on cc_incident.id_stg = cc_assessmentcontentitem.IncidentID_stg 											

left join DB_T_PROD_STAG.cctl_contentlineitemcategory 											

    on cc_assessmentcontentitem.ContentCategory_stg = cctl_contentlineitemcategory.ID_stg											

left join DB_T_PROD_STAG.cctl_contentlineitemschedule 											

    on cc_assessmentcontentitem.ContentSchedule_stg = cctl_contentlineitemschedule.ID_stg											

where    cc_assessmentcontentitem.UpdateTime_stg > (:START_DTTM) 											

AND cc_assessmentcontentitem.UpdateTime_stg <= (:END_DTTM)											

AND cctl_incident.name_stg=''PropertyContentsIncident'' 											

											

UNION											

											

/* BUILDING	EIM-35253										 */
SELECT distinct											

    cast(a.fixedid_stg as varchar(100)) as id,											

    cast(''PRTY_ASSET_SBTYPE32'' as varchar(50)) as assettype,											

    cast(''PRTY_ASSET_CLASFCN10'' as varchar(50))as classification_code,											

    ''SRC_SYS4'' as src_cd,											

    '''' as asset_name_stg,											

b.Description_stg as asset_desc ,/*  Changes to asset decription Column											 */
    cast(null as varchar(100)) as reg_num,											

    a.createtime_stg as CreateTime											

    ,coalesce(a.expirationdate_stg, cast(''9999-12-31 23:59:59.000000'' AS TIMESTAMP(6))) as expirationdate											

from DB_T_PROD_STAG.pcx_bp7building a											

join DB_T_PROD_STAG.PC_BUILDING b on b.FixedID_stg = a.Building_stg and b.BranchID_stg = a.BranchID_stg											

where (a.expirationdate_stg is null or a.expirationdate_stg >= current_timestamp)

and (b.expirationdate_stg is null or b.expirationdate_stg >= current_timestamp)								

and ((a.updatetime_stg > (:START_DTTM)  AND a.updatetime_stg <= (:END_DTTM))											

    OR (b.updatetime_stg > (:START_DTTM) AND b.updatetime_stg <= (:END_DTTM)))											

UNION

/* ------------- EIM-48788 FARM CHANGES------------------------------------------- */
/* FOP DWELLING */


select distinct cast(pcx_fopdwelling.fixedid_stg  as varchar(100))as id,

''PRTY_ASSET_SBTYPE37'' as assettype ,''PRTY_ASSET_CLASFCN15'' as classification_code,''SRC_SYS4'' as src_cd

,'''' as asset_name , '''' as asset_desc,cast(null as varchar(100)) as reg_num

,  pcx_fopdwelling.createtime_stg as CreateTime, 

case when pcx_fopdwelling.ExpirationDate_stg is null then cast(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6)) 

else pcx_fopdwelling.ExpirationDate_stg end as expirationdate

from DB_T_PROD_STAG.pcx_fopdwelling 

where pcx_fopdwelling.fixedid_stg is not null

and pcx_fopdwelling.UpdateTime_stg >(:START_DTTM) 

AND  pcx_fopdwelling.UpdateTime_stg <= (:END_DTTM)



  

UNION



/* FOP OUTBUILDING */
select distinct cast(pcx_fopoutbuilding.fixedid_stg  as varchar(100))as id ,

cast(''PRTY_ASSET_SBTYPE36'' as varchar(50)) as assettype ,

cast(''PRTY_ASSET_CLASFCN13'' as varchar(50)) as classification_code,''SRC_SYS4'' as src_cd

,cast('''' as varchar(255)) as asset_name , cast('''' as varchar(255)) as asset_desc,cast(null as varchar(100)) as reg_num

, pcx_fopoutbuilding.createtime_stg as CreateTime,

case when pcx_fopoutbuilding.expirationdate_stg is null then cast(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6))  

else pcx_fopoutbuilding.expirationdate_stg end as expirationdate

from DB_T_PROD_STAG.pcx_fopoutbuilding

where pcx_fopoutbuilding.fixedid_stg is not null

and pcx_fopoutbuilding.UpdateTime_stg >(:START_DTTM)

and pcx_fopoutbuilding.UpdateTime_stg <= (:END_DTTM)



UNION



/* FOP FEEDANDSEED */
select distinct cast(pcx_fopfeedandseed.fixedid_stg  as varchar(100))as id ,

cast(''PRTY_ASSET_SBTYPE33'' as varchar(50)) as assettype ,

cast(''PRTY_ASSET_CLASFCN11'' as varchar(50)) as classification_code,''SRC_SYS4'' as src_cd

,cast('''' as varchar(255)) as asset_name , cast('''' as varchar(255)) as asset_desc,cast(null as varchar(100)) as reg_num

, pcx_fopfeedandseed.createtime_stg as CreateTime,

case when pcx_fopfeedandseed.expirationdate_stg is null then cast(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6)) 

else pcx_fopfeedandseed.expirationdate_stg end as expirationdate

from DB_T_PROD_STAG.pcx_fopfeedandseed

where pcx_fopfeedandseed.fixedid_stg is not null

and pcx_fopfeedandseed.UpdateTime_stg >(:START_DTTM)

and pcx_fopfeedandseed.UpdateTime_stg <= (:END_DTTM)



UNION



/* FOP LIVESTOCK */
select distinct cast(pcx_foplivestock.fixedid_stg  as varchar(100))as id ,

cast(''PRTY_ASSET_SBTYPE35'' as varchar(50)) as assettype ,

cast(''PRTY_ASSET_CLASFCN14'' as varchar(50)) as classification_code,''SRC_SYS4'' as src_cd

,cast('''' as varchar(255)) as asset_name , cast('''' as varchar(255)) as asset_desc,cast(null as varchar(100)) as reg_num

, pcx_foplivestock.createtime_stg as CreateTime,

case when pcx_foplivestock.expirationdate_stg is null then cast(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6))   

else pcx_foplivestock.expirationdate_stg end as expirationdate

from DB_T_PROD_STAG.pcx_foplivestock

where pcx_foplivestock.fixedid_stg is not null

and pcx_foplivestock.UpdateTime_stg >(:START_DTTM)

and pcx_foplivestock.UpdateTime_stg <= (:END_DTTM)



UNION



/* FOP MACHINERY */
select distinct cast(pcx_fopmachinery.fixedid_stg  as varchar(100))as id ,

cast(''PRTY_ASSET_SBTYPE34'' as varchar(50)) as assettype ,

cast(''PRTY_ASSET_CLASFCN12'' as varchar(50)) as classification_code,''SRC_SYS4'' as src_cd

,cast('''' as varchar(255)) as asset_name , cast('''' as varchar(255)) as asset_desc,cast(null as varchar(100)) as reg_num

, pcx_fopmachinery.createtime_stg as CreateTime,

case when pcx_fopmachinery.expirationdate_stg is null then cast(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6))   

else pcx_fopmachinery.expirationdate_stg end as expirationdate

from DB_T_PROD_STAG.pcx_fopmachinery

where pcx_fopmachinery.fixedid_stg is not null

and pcx_fopmachinery.UpdateTime_stg >(:START_DTTM)

and pcx_fopmachinery.UpdateTime_stg <= (:END_DTTM)



UNION



/* FOP DWELLINGSCHDCOVITEM */
select distinct cast(pcx_fopdwellingschdcovitem.fixedid_stg  as varchar(100))as id ,

cast(''PRTY_ASSET_SBTYPE38'' as varchar(50)) as assettype ,

cast(''PRTY_ASSET_CLASFCN16'' as varchar(50)) as classification_code,''SRC_SYS4'' as src_cd

,cast('''' as varchar(255)) as asset_name , cast( (ltrim(COALESCE(StringCol1_stg,'''')||'' ''||COALESCE(StringCol2_stg,'''')||'' ''||COALESCE(StringCol3_stg,'''')||'' ''||COALESCE(StringCol4_stg,'''')||'' ''||

COALESCE(TypeKeyCol1_stg,'''')||'' ''||COALESCE(TypeKeyCol1_stg,''''))) as varchar(255)) as asset_desc,cast(null as varchar(100)) as reg_num

, pcx_fopdwellingschdcovitem.createtime_stg as CreateTime,

case when pcx_fopdwellingschdcovitem.expirationdate_stg is null then cast(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6))   

else pcx_fopdwellingschdcovitem.expirationdate_stg end as expirationdate

from DB_T_PROD_STAG.pcx_fopdwellingschdcovitem

where pcx_fopdwellingschdcovitem.fixedid_stg is not null

and pcx_fopdwellingschdcovitem.UpdateTime_stg >(:START_DTTM)

and pcx_fopdwellingschdcovitem.UpdateTime_stg <= (:END_DTTM)



UNION



/* FOP DWELLINGSCHDEXCLITEM */
select distinct cast(pcx_fopdwellingschdexclitem.fixedid_stg  as varchar(100))as id ,

cast(''PRTY_ASSET_SBTYPE40'' as varchar(50)) as assettype ,

cast(''PRTY_ASSET_CLASFCN18'' as varchar(50)) as classification_code,''SRC_SYS4'' as src_cd

,cast('''' as varchar(255)) as asset_name , cast( (ltrim(COALESCE(StringCol1_stg,'''')||'' ''||COALESCE(StringCol2_stg,'''')||'' ''||COALESCE(StringCol3_stg,'''')||'' ''||COALESCE(StringCol4_stg,'''')||'' ''||

COALESCE(TypeKeyCol1_stg,'''')||'' ''||COALESCE(TypeKeyCol1_stg,''''))) as varchar(255)) as asset_desc,cast(null as varchar(100)) as reg_num

, pcx_fopdwellingschdexclitem.createtime_stg as CreateTime,

case when pcx_fopdwellingschdexclitem.expirationdate_stg is null then cast(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6))  

else pcx_fopdwellingschdexclitem.expirationdate_stg end as expirationdate

from DB_T_PROD_STAG.pcx_fopdwellingschdexclitem

where pcx_fopdwellingschdexclitem.fixedid_stg is not null

and pcx_fopdwellingschdexclitem.UpdateTime_stg >(:START_DTTM)

and pcx_fopdwellingschdexclitem.UpdateTime_stg <= (:END_DTTM)



/* FOP FARMOWNERSLISCHDCOVITEM */


UNION



select distinct cast(pcx_fopfarmownerslischdcovitem.fixedid_stg  as varchar(100))as id ,

cast(''PRTY_ASSET_SBTYPE41'' as varchar(50)) as assettype ,

cast(''PRTY_ASSET_CLASFCN19'' as varchar(50)) as classification_code,''SRC_SYS4'' as src_cd

,cast('''' as varchar(255)) as asset_name , cast( (ltrim(COALESCE(StringCol1_stg,'''')||'' ''||COALESCE(StringCol2_stg,'''')||'' ''||COALESCE(StringCol3_stg,'''')||'' ''||COALESCE(StringCol4_stg,'''')||'' ''||

COALESCE(TypeKeyCol1_stg,'''')||'' ''||COALESCE(TypeKeyCol1_stg,''''))) as varchar(255)) as asset_desc

,cast(null as varchar(100)) as reg_num

, pcx_fopfarmownerslischdcovitem.createtime_stg as CreateTime,

case when pcx_fopfarmownerslischdcovitem.expirationdate_stg is null then cast(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6))  

else pcx_fopfarmownerslischdcovitem.expirationdate_stg end as expirationdate

from DB_T_PROD_STAG.pcx_fopfarmownerslischdcovitem

where pcx_fopfarmownerslischdcovitem.fixedid_stg is not null

and pcx_fopfarmownerslischdcovitem.UpdateTime_stg >(:START_DTTM) 

and pcx_fopfarmownerslischdcovitem.UpdateTime_stg <= ( :END_DTTM)



/* FOP LIABILITYSCHDCOVITEM */


UNION



select distinct cast(pcx_fopliabilityschdcovitem.fixedid_stg  as varchar(100))as id ,

cast(''PRTY_ASSET_SBTYPE42'' as varchar(50)) as assettype ,

cast(''PRTY_ASSET_CLASFCN20'' as varchar(50)) as classification_code,''SRC_SYS4'' as src_cd

,cast('''' as varchar(255)) as asset_name , cast( (ltrim(COALESCE(StringCol1_stg,'''')||'' ''||COALESCE(StringCol2_stg,'''')||'' ''||COALESCE(StringCol3_stg,'''')||'' ''||COALESCE(StringCol4_stg,'''')||'' ''||

COALESCE(TypeKeyCol1_stg,'''')||'' ''||COALESCE(TypeKeyCol1_stg,''''))) as varchar(255)) as asset_desc

,cast(null as varchar(100)) as reg_num

, pcx_fopliabilityschdcovitem.createtime_stg as CreateTime,

case when pcx_fopliabilityschdcovitem.expirationdate_stg is null then cast(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6))  

else pcx_fopliabilityschdcovitem.expirationdate_stg end as expirationdate

from DB_T_PROD_STAG.pcx_fopliabilityschdcovitem

where pcx_fopliabilityschdcovitem.fixedid_stg is not null

and pcx_fopliabilityschdcovitem.UpdateTime_stg >(:START_DTTM) 

and pcx_fopliabilityschdcovitem.UpdateTime_stg <= ( :END_DTTM)



/* -------------------------------- FARM CHANGES END--------------- */


) as TMP 											
qualify row_number() over(partition by ltrim(rtrim(ID)),assettype,classification_code,src_cd  order by expirationdate desc,CreateTime DESC)=1	
 order by id asc,assettype asc,classification_code asc,CreateTime asc,expirationdate asc
) SRC
)
);


-- Component exp_derive_src_cd, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_derive_src_cd AS
(
SELECT
ltrim ( rtrim ( sq_cc_vehicle.Asset_Key ) ) as Asset_Key1,
CASE WHEN sq_cc_vehicle.Asset_Name IS NULL or ltrim ( rtrim ( sq_cc_vehicle.Asset_Name ) ) = '''' THEN ''UNK '' ELSE sq_cc_vehicle.Asset_Name END as Asset_Name1,
CASE WHEN sq_cc_vehicle.Asset_Desc IS NULL or ltrim ( rtrim ( sq_cc_vehicle.Asset_Desc ) ) = '''' THEN ''UNK '' ELSE sq_cc_vehicle.Asset_Desc END as Asset_Desc1,
sq_cc_vehicle.Createtime as Createtime,
sq_cc_vehicle.expirationdate as expirationdate,
CASE WHEN LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE */ IS NULL THEN ''UNK'' ELSE LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE */ END as out_Asset_Type,
CASE WHEN LKP_3.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN */ IS NULL THEN ''UNK'' ELSE LKP_4.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN */ END as out_Classification_Cd,
LKP_5.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SRC_CD */ as o_SYS_SRC_CD,
:PRCS_ID as PRCS_ID,
CURRENT_TIMESTAMP as EDW_STRT_DTTM,
TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as EDW_END_DTTM,
dateadd ( second, -1, CURRENT_TIMESTAMP  ) as EDW_expiry,
NULL as in_PRTY_ASSET_ID,
sq_cc_vehicle.reg_num as reg_num,
sq_cc_vehicle.source_record_id,
row_number() over (partition by sq_cc_vehicle.source_record_id order by sq_cc_vehicle.source_record_id) as RNK
FROM
sq_cc_vehicle
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = sq_cc_vehicle.Asset_Type
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = sq_cc_vehicle.Asset_Type
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN LKP_3 ON LKP_3.SRC_IDNTFTN_VAL = sq_cc_vehicle.Classification_Cd
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN LKP_4 ON LKP_4.SRC_IDNTFTN_VAL = sq_cc_vehicle.Classification_Cd
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_SRC_CD LKP_5 ON LKP_5.SRC_IDNTFTN_VAL = sq_cc_vehicle.SRC_SYS_CD
QUALIFY RNK = 1
);


-- Component LKP_PRTY_ASSET, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_PRTY_ASSET AS
(
SELECT
LKP.PRTY_ASSET_ID,
LKP.PRTY_ASSET_SBTYPE_CD,
LKP.ASSET_DESC,
LKP.PRTY_ASSET_NAME,
LKP.ASSET_HOST_ID_VAL,
LKP.PRTY_ASSET_CLASFCN_CD,
LKP.PRTY_ASSET_STRT_DTTM,
LKP.PRTY_ASSET_END_DTTM,
LKP.EDW_STRT_DTTM,
exp_derive_src_cd.Asset_Key1 as Asset_Key,
exp_derive_src_cd.out_Asset_Type as out_Asset_Type,
exp_derive_src_cd.out_Classification_Cd as out_Classification_Cd,
exp_derive_src_cd.Asset_Name1 as Asset_Name,
exp_derive_src_cd.Asset_Desc1 as Asset_Desc1,
exp_derive_src_cd.Createtime as Createtime,
exp_derive_src_cd.expirationdate as expirationdate,
exp_derive_src_cd.PRCS_ID as PRCS_ID1,
exp_derive_src_cd.EDW_STRT_DTTM as EDW_STRT_DTTM1,
exp_derive_src_cd.EDW_END_DTTM as EDW_END_DTTM1,
exp_derive_src_cd.EDW_expiry as EDW_expiry,
exp_derive_src_cd.o_SYS_SRC_CD as o_SYS_SRC_CD,
exp_derive_src_cd.in_PRTY_ASSET_ID as in_PRTY_ASSET_ID,
exp_derive_src_cd.reg_num as reg_num,
exp_derive_src_cd.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_derive_src_cd.source_record_id ORDER BY LKP.PRTY_ASSET_ID asc,LKP.PRTY_ASSET_SBTYPE_CD asc,LKP.ASSET_DESC asc,LKP.PRTY_ASSET_NAME asc,LKP.ASSET_HOST_ID_VAL asc,LKP.PRTY_ASSET_CLASFCN_CD asc,LKP.PRTY_ASSET_STRT_DTTM asc,LKP.PRTY_ASSET_END_DTTM asc,LKP.EDW_STRT_DTTM asc) RNK
FROM
exp_derive_src_cd
LEFT JOIN (
SELECT PRTY_ASSET.PRTY_ASSET_ID as PRTY_ASSET_ID, PRTY_ASSET.ASSET_DESC as ASSET_DESC, PRTY_ASSET.PRTY_ASSET_NAME as PRTY_ASSET_NAME, 
PRTY_ASSET.PRTY_ASSET_STRT_DTTM as PRTY_ASSET_STRT_DTTM, PRTY_ASSET.PRTY_ASSET_END_DTTM as PRTY_ASSET_END_DTTM, 
PRTY_ASSET.EDW_STRT_DTTM as EDW_STRT_DTTM, 
ltrim(rtrim(PRTY_ASSET.ASSET_HOST_ID_VAL)) as ASSET_HOST_ID_VAL, PRTY_ASSET.PRTY_ASSET_SBTYPE_CD as PRTY_ASSET_SBTYPE_CD,
 PRTY_ASSET.PRTY_ASSET_CLASFCN_CD as PRTY_ASSET_CLASFCN_CD FROM db_t_prod_core.PRTY_ASSET 
 QUALIFY ROW_NUMBER() OVER(PARTITION BY ASSET_HOST_ID_VAL,PRTY_ASSET_SBTYPE_CD,PRTY_ASSET_CLASFCN_CD  ORDER BY EDW_END_DTTM desc) = 1
) LKP ON LKP.ASSET_HOST_ID_VAL = exp_derive_src_cd.Asset_Key1 AND LKP.PRTY_ASSET_SBTYPE_CD = exp_derive_src_cd.out_Asset_Type AND LKP.PRTY_ASSET_CLASFCN_CD = exp_derive_src_cd.out_Classification_Cd
QUALIFY RNK = 1
);


-- Component exp_data_transformation, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_data_transformation AS
(
SELECT
LKP_PRTY_ASSET.PRTY_ASSET_ID as LKP_PRTY_ASSET_ID,
1 as CNTL_ID,
LKP_PRTY_ASSET.PRCS_ID1 as PRCS_ID,
LKP_PRTY_ASSET.Asset_Key as Asset_key,
exp_derive_src_cd.out_Asset_Type as Asset_type,
exp_derive_src_cd.out_Classification_Cd as Classification_cd,
LKP_PRTY_ASSET.Asset_Name as Asset_Name,
:p_default_str_cd as v_ASSET_INSRNC_HIST_TYPE_CD,
LKP_PRTY_ASSET.Asset_Desc1 as Asset_Desc,
LKP_PRTY_ASSET.Createtime as Createtime,
CASE WHEN LKP_PRTY_ASSET.Createtime IS NULL THEN to_date ( ''01/01/1900'' , ''mm/dd/yyyy'' ) ELSE LKP_PRTY_ASSET.Createtime END as v_Createtime,
v_Createtime as o_Createtime,
LKP_PRTY_ASSET.expirationdate as expirationdate,
LKP_PRTY_ASSET.ASSET_DESC as lkp_ASSET_DESC1,
LKP_PRTY_ASSET.PRTY_ASSET_NAME as lkp_PRTY_ASSET_NAME,
LKP_PRTY_ASSET.PRTY_ASSET_END_DTTM as lkp_PRTY_ASSET_END_DT,
LKP_PRTY_ASSET.EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
LKP_PRTY_ASSET.EDW_STRT_DTTM1 as EDW_STRT_DTTM,
LKP_PRTY_ASSET.EDW_END_DTTM1 as EDW_END_DTTM,
LKP_PRTY_ASSET.EDW_expiry as EDW_END_DTTM_exp,
LKP_PRTY_ASSET.PRTY_ASSET_STRT_DTTM as lkp_PRTY_ASSET_STRT_DT,
LKP_PRTY_ASSET.PRTY_ASSET_SBTYPE_CD as lkp_PRTY_ASSET_SBTYPE_CD,
MD5 ( rtrim ( ltrim ( upper ( LKP_PRTY_ASSET.ASSET_DESC ) ) ) || rtrim ( ltrim ( upper ( LKP_PRTY_ASSET.PRTY_ASSET_NAME ) ) ) || TO_CHAR ( LKP_PRTY_ASSET.PRTY_ASSET_STRT_DTTM ) || TO_CHAR ( LKP_PRTY_ASSET.PRTY_ASSET_END_DTTM ) ) as chksum_lkp,
md5 ( ltrim ( rtrim ( upper ( LKP_PRTY_ASSET.Asset_Desc1 ) ) ) || ltrim ( rtrim ( upper ( LKP_PRTY_ASSET.Asset_Name ) ) ) || to_char ( v_Createtime ) || TO_CHAR ( LKP_PRTY_ASSET.expirationdate ) ) as chksum_inp,
CASE WHEN chksum_lkp IS NULL THEN ''I'' ELSE CASE WHEN chksum_lkp != chksum_inp THEN ''U'' ELSE ''R'' END END as out_INS_UPD_flag,
LKP_PRTY_ASSET.o_SYS_SRC_CD as o_SYS_SRC_CD,
LKP_PRTY_ASSET.ASSET_HOST_ID_VAL as lkp_ASSET_HOST_ID_VAL,
LKP_PRTY_ASSET.PRTY_ASSET_CLASFCN_CD as lkp_PRTY_ASSET_CLASFCN_CD,
LKP_PRTY_ASSET.in_PRTY_ASSET_ID as in_PRTY_ASSET_ID,
-- CASE WHEN LKP_PRTY_ASSET.Asset_Key = LKP_PRTY_ASSET.ASSET_HOST_ID_VAL AND exp_derive_src_cd.out_Classification_Cd = LKP_PRTY_ASSET.PRTY_ASSET_CLASFCN_CD and exp_derive_src_cd.out_Asset_Type = LKP_PRTY_ASSET.PRTY_ASSET_SBTYPE_CD THEN v_prty_asset_id ELSE 0 END as V_INC,
-- V_INC as V_VAR,
-- CASE WHEN LKP_PRTY_ASSET.Asset_Key = LKP_PRTY_ASSET.ASSET_HOST_ID_VAL AND exp_derive_src_cd.out_Classification_Cd = LKP_PRTY_ASSET.PRTY_ASSET_CLASFCN_CD and exp_derive_src_cd.out_Asset_Type = LKP_PRTY_ASSET.PRTY_ASSET_SBTYPE_CD THEN V_VAR ELSE LKP_PRTY_ASSET.in_PRTY_ASSET_ID END as v_prty_asset_id,
LAST_VALUE(CASE WHEN LKP_PRTY_ASSET.Asset_Key = LKP_PRTY_ASSET.ASSET_HOST_ID_VAL AND exp_derive_src_cd.out_Classification_Cd = LKP_PRTY_ASSET.PRTY_ASSET_CLASFCN_CD and exp_derive_src_cd.out_Asset_Type = LKP_PRTY_ASSET.PRTY_ASSET_SBTYPE_CD THEN LKP_PRTY_ASSET.in_PRTY_ASSET_ID ELSE NULL END IGNORE NULLS) OVER (ORDER BY LKP_PRTY_ASSET.source_record_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) as V_INC,
V_INC as V_VAR,
CASE WHEN LKP_PRTY_ASSET.Asset_Key = LKP_PRTY_ASSET.ASSET_HOST_ID_VAL AND exp_derive_src_cd.out_Classification_Cd = LKP_PRTY_ASSET.PRTY_ASSET_CLASFCN_CD and exp_derive_src_cd.out_Asset_Type = LKP_PRTY_ASSET.PRTY_ASSET_SBTYPE_CD THEN LAST_VALUE(CASE WHEN LKP_PRTY_ASSET.Asset_Key = LKP_PRTY_ASSET.ASSET_HOST_ID_VAL AND exp_derive_src_cd.out_Classification_Cd = LKP_PRTY_ASSET.PRTY_ASSET_CLASFCN_CD and exp_derive_src_cd.out_Asset_Type = LKP_PRTY_ASSET.PRTY_ASSET_SBTYPE_CD THEN LKP_PRTY_ASSET.in_PRTY_ASSET_ID ELSE NULL END IGNORE NULLS) OVER (ORDER BY LKP_PRTY_ASSET.source_record_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) ELSE LKP_PRTY_ASSET.in_PRTY_ASSET_ID END as v_prty_asset_id,
LKP_PRTY_ASSET.reg_num as reg_num,
exp_derive_src_cd.source_record_id
FROM
exp_derive_src_cd
INNER JOIN LKP_PRTY_ASSET ON exp_derive_src_cd.source_record_id = LKP_PRTY_ASSET.source_record_id
);


-- Component rtr_prty_asset_INSERT, Type ROUTER Output Group INSERT
create or replace temporary table rtr_prty_asset_INSERT as
SELECT
exp_data_transformation.CNTL_ID as CNTL_ID,
exp_data_transformation.PRCS_ID as PRCS_ID,
exp_data_transformation.Asset_key as Asset_key,
exp_data_transformation.Asset_type as Asset_type,
exp_data_transformation.LKP_PRTY_ASSET_ID as LKP_PRTY_ASSET_ID,
exp_data_transformation.Classification_cd as Class_Cd,
exp_data_transformation.Asset_Name as Asset_Name,
exp_data_transformation.Asset_Desc as Asset_Desc,
exp_data_transformation.out_INS_UPD_flag as out_INS_UPD_flag,
exp_data_transformation.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_data_transformation.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_data_transformation.EDW_END_DTTM as EDW_END_DTTM,
exp_data_transformation.EDW_END_DTTM_exp as EDW_END_DTTM_exp,
exp_data_transformation.o_SYS_SRC_CD as o_SYS_SRC_CD,
exp_data_transformation.o_Createtime as o_Createtime,
exp_data_transformation.expirationdate as expirationdate,
exp_data_transformation.lkp_ASSET_DESC1 as lkp_ASSET_DESC1,
exp_data_transformation.lkp_PRTY_ASSET_NAME as lkp_PRTY_ASSET_NAME,
exp_data_transformation.lkp_PRTY_ASSET_END_DT as lkp_PRTY_ASSET_END_DT,
exp_data_transformation.lkp_PRTY_ASSET_SBTYPE_CD as lkp_PRTY_ASSET_SBTYPE_CD,
exp_data_transformation.lkp_PRTY_ASSET_STRT_DT as lkp_PRTY_ASSET_STRT_DT,
exp_data_transformation.lkp_ASSET_HOST_ID_VAL as lkp_ASSET_HOST_ID_VAL,
exp_data_transformation.lkp_PRTY_ASSET_CLASFCN_CD as lkp_PRTY_ASSET_CLASFCN_CD,
exp_data_transformation.LKP_PRTY_ASSET_ID as in_PRTY_ASSET_ID,
exp_data_transformation.reg_num as reg_num,
exp_data_transformation.source_record_id
FROM
exp_data_transformation
WHERE exp_data_transformation.out_INS_UPD_flag = ''I'' or exp_data_transformation.out_INS_UPD_flag = ''U'';


-- Component upd_ins_prty_asset, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_ins_prty_asset AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_prty_asset_INSERT.Asset_key as Asset_key,
rtr_prty_asset_INSERT.Asset_type as Asset_type,
rtr_prty_asset_INSERT.in_PRTY_ASSET_ID as PRTY_ASSET_ID,
NULL as CNTL_ID,
rtr_prty_asset_INSERT.PRCS_ID as PRCS_ID,
rtr_prty_asset_INSERT.Class_Cd as Class_Cd1,
rtr_prty_asset_INSERT.Asset_Name as Asset_Name1,
rtr_prty_asset_INSERT.Asset_Desc as Asset_Desc1,
rtr_prty_asset_INSERT.EDW_STRT_DTTM as EDW_STRT_DTTM,
rtr_prty_asset_INSERT.EDW_END_DTTM as EDW_END_DTTM,
rtr_prty_asset_INSERT.o_SYS_SRC_CD as o_SYS_SRC_CD1,
rtr_prty_asset_INSERT.o_Createtime as o_Createtime1,
rtr_prty_asset_INSERT.expirationdate as expirationdate3,
rtr_prty_asset_INSERT.out_INS_UPD_flag as out_INS_UPD_flag1,
rtr_prty_asset_INSERT.reg_num as reg_num1,
0 as UPDATE_STRATEGY_ACTION,
source_record_id
FROM
rtr_prty_asset_INSERT
);


-- Component exp_pass_to_target, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_target AS
(
SELECT
upd_ins_prty_asset.Asset_key as Asset_key,
:p_default_str_cd as ASSET_INSRNC_HIST_TYPE_CD,
upd_ins_prty_asset.Asset_type as Asset_type,
upd_ins_prty_asset.Class_Cd1 as Class_Cd1,
LKP_1.PRTY_ASSET_ID /* replaced lookup LKP_XREF_PRTY_ASSET */ as PRTY_ASSET_ID,
upd_ins_prty_asset.PRCS_ID as PRCS_ID,
upd_ins_prty_asset.Asset_Name1 as Asset_Name1,
upd_ins_prty_asset.Asset_Desc1 as Asset_Desc1,
upd_ins_prty_asset.EDW_STRT_DTTM as EDW_STRT_DTTM,
upd_ins_prty_asset.EDW_END_DTTM as EDW_END_DTTM,
upd_ins_prty_asset.o_SYS_SRC_CD1 as o_SYS_SRC_CD1,
upd_ins_prty_asset.o_Createtime1 as o_Createtime1,
upd_ins_prty_asset.expirationdate3 as expirationdate3,
upd_ins_prty_asset.out_INS_UPD_flag1 as out_INS_UPD_flag1,
upd_ins_prty_asset.PRTY_ASSET_ID as PRTY_ASSET_ID1,
upd_ins_prty_asset.reg_num1 as reg_num1,
upd_ins_prty_asset.source_record_id,
row_number() over (partition by upd_ins_prty_asset.source_record_id order by upd_ins_prty_asset.source_record_id) as RNK
FROM
upd_ins_prty_asset
LEFT JOIN LKP_XREF_PRTY_ASSET LKP_1 ON LKP_1.PRTY_ASSET_SBTYPE_CD = upd_ins_prty_asset.Asset_type AND LKP_1.ASSET_HOST_ID_VAL = upd_ins_prty_asset.Asset_key AND LKP_1.PRTY_ASSET_CLASFCN_CD = upd_ins_prty_asset.Class_Cd1
QUALIFY RNK = 1
);


-- Component exp_PRTY_ASSET_ID, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_PRTY_ASSET_ID AS
(
SELECT
CASE WHEN exp_pass_to_target.out_INS_UPD_flag1 = ''I'' THEN exp_pass_to_target.PRTY_ASSET_ID ELSE exp_pass_to_target.PRTY_ASSET_ID1 END as out_PRTY_ASSET_ID,
exp_pass_to_target.reg_num1 as reg_num1,
exp_pass_to_target.source_record_id
FROM
exp_pass_to_target
);


-- Component tgt_PRTY_ASSET_ins, Type TARGET 
INSERT INTO DB_T_PROD_CORE.PRTY_ASSET
(
PRTY_ASSET_ID,
ASSET_INSRNC_HIST_TYPE_CD,
PRTY_ASSET_SBTYPE_CD,
ASSET_DESC,
PRTY_ASSET_NAME,
ASSET_RGSTRN_NUM,
ASSET_HOST_ID_VAL,
PRTY_ASSET_CLASFCN_CD,
PRCS_ID,
PRTY_ASSET_STRT_DTTM,
PRTY_ASSET_END_DTTM,
EDW_STRT_DTTM,
EDW_END_DTTM,
SRC_SYS_CD
)
SELECT
exp_PRTY_ASSET_ID.out_PRTY_ASSET_ID as PRTY_ASSET_ID,
exp_pass_to_target.ASSET_INSRNC_HIST_TYPE_CD as ASSET_INSRNC_HIST_TYPE_CD,
exp_pass_to_target.Asset_type as PRTY_ASSET_SBTYPE_CD,
left(exp_pass_to_target.Asset_Desc1,250) as ASSET_DESC,
exp_pass_to_target.Asset_Name1 as PRTY_ASSET_NAME,
exp_PRTY_ASSET_ID.reg_num1 as ASSET_RGSTRN_NUM,
exp_pass_to_target.Asset_key as ASSET_HOST_ID_VAL,
exp_pass_to_target.Class_Cd1 as PRTY_ASSET_CLASFCN_CD,
exp_pass_to_target.PRCS_ID as PRCS_ID,
exp_pass_to_target.o_Createtime1 as PRTY_ASSET_STRT_DTTM,
exp_pass_to_target.expirationdate3 as PRTY_ASSET_END_DTTM,
exp_pass_to_target.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_pass_to_target.EDW_END_DTTM as EDW_END_DTTM,
exp_pass_to_target.o_SYS_SRC_CD1 as SRC_SYS_CD
FROM
exp_pass_to_target
INNER JOIN exp_PRTY_ASSET_ID ON exp_pass_to_target.source_record_id = exp_PRTY_ASSET_ID.source_record_id;


-- Component tgt_PRTY_ASSET_ins, Type Post SQL 
UPDATE db_t_prod_core.PRTY_ASSET
SET EDW_END_DTTM = A.lead1
FROM (

	SELECT DISTINCT ASSET_HOST_ID_VAL

		,PRTY_ASSET_SBTYPE_CD

		,PRTY_ASSET_CLASFCN_CD

		,EDW_STRT_DTTM

		,max(EDW_STRT_DTTM) OVER (

			PARTITION BY ASSET_HOST_ID_VAL

			,PRTY_ASSET_SBTYPE_CD

			,PRTY_ASSET_CLASFCN_CD ORDER BY EDW_STRT_DTTM ASC rows BETWEEN 1 following



					AND 1 following

			) - INTERVAL ''1 SECOND'' AS lead1

	FROM DB_T_PROD_CORE.PRTY_ASSET

	GROUP BY ASSET_HOST_ID_VAL

		,PRTY_ASSET_SBTYPE_CD

		,PRTY_ASSET_CLASFCN_CD

		,EDW_STRT_DTTM

	) a





WHERE PRTY_ASSET.EDW_STRT_DTTM = A.EDW_STRT_DTTM

	AND PRTY_ASSET.ASSET_HOST_ID_VAL = A.ASSET_HOST_ID_VAL

	AND PRTY_ASSET.PRTY_ASSET_SBTYPE_CD = A.PRTY_ASSET_SBTYPE_CD

	AND PRTY_ASSET.PRTY_ASSET_CLASFCN_CD = A.PRTY_ASSET_CLASFCN_CD

	AND CAST(PRTY_ASSET.EDW_END_DTTM AS DATE) = ''9999-12-31''

	AND lead1 IS NOT NULL;


END; 
';