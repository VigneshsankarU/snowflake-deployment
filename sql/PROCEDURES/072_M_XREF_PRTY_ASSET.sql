-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_XREF_PRTY_ASSET("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
DECLARE
  start_dttm STRING;
  end_dttm STRING;
BEGIN
  SELECT 
    TRY_PARSE_JSON(:param_json):start_dttm::STRING,
    TRY_PARSE_JSON(:param_json):end_dttm::STRING
  INTO
    start_dttm,
    end_dttm;

-- Component LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

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

FROM DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

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

FROM DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''SRC_SYS'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component SQ_XREF_PRTY_ASSET, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_XREF_PRTY_ASSET AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as PRTY_ASSET_SBTYPE_CD,
$2 as ASSET_HOST_ID_VAL,
$3 as PRTY_ASSET_CLASFCN_CD,
$4 as SRC_SYS_CD,
$5 as LOAD_DTTM,
$6 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select distinct assettype,								

LTRIM(rtrim(ID)) id,								

classification_code,								

src_cd,								

CURRENT_timestamp as load_dttm								

 from (								

/** DB_T_CORE_PROD.VEHICLE **/								

/**watercraftmotor**/								

								

select distinct cast(fixedid_stg  as varchar(100))as id , cast(''PRTY_ASSET_SBTYPE4'' as varchar(50)) as assettype ,								

cast(''PRTY_ASSET_CLASFCN4'' as varchar(50)) as classification_code,''SRC_SYS4'' as src_cd								

from DB_T_PROD_STAG.pcx_pawatercraftmotor_alfa 								

where fixedid_stg is not null 								

and pcx_pawatercraftmotor_alfa.UpdateTime_stg>(:start_dttm) 								

and pcx_pawatercraftmotor_alfa.UpdateTime_stg <= ( :end_dttm)								

								

UNION								

/**WaterCrafttrailer**/								

select distinct cast(fixedid_stg  as varchar(100))as id , ''PRTY_ASSET_SBTYPE4''  as assettype ,''PRTY_ASSET_CLASFCN5'' as classification_code,''SRC_SYS4'' as src_cd								

from DB_T_PROD_STAG.pcx_pawatercrafttrailer_alfa 								

where fixedid_stg is not null								

and pcx_pawatercrafttrailer_alfa.UpdateTime_stg>(:start_dttm)								

and pcx_pawatercrafttrailer_alfa.UpdateTime_stg <= ( :end_dttm)								

								

UNION								

/**Motor Vehicle**/								

select distinct cast(pc_personalvehicle.fixedid_stg  as varchar(100))as id 								

, ''PRTY_ASSET_SBTYPE4''  as assettype ,''PRTY_ASSET_CLASFCN3'' as classification_code,''SRC_SYS4'' as src_cd								

from 								

DB_T_PROD_STAG.pc_personalvehicle								

where pc_personalvehicle.fixedid_stg is not null and 								

 pc_personalvehicle.UpdateTime_stg> (:start_dttm)								

    and pc_personalvehicle.UpdateTime_stg <= ( :end_dttm)								

   								

								

UNION								

/**Motor DB_T_CORE_PROD.VEHICLE Third party/Unverified**/								

select distinct 								

cast((case 								

when (cc_vehicle.PolicySystemId_stg is null and cc_vehicle.Vin_stg is not null) then  ''VIN:''|| cc_vehicle.vin_stg 								

when (cc_vehicle.PolicySystemId_stg is null and cc_vehicle.Vin_stg is null and cc_vehicle.LicensePlate_stg is not null) then ''LP:'' || cc_vehicle.licenseplate_stg								

when (cc_vehicle.PolicySystemId_stg is null and cc_vehicle.Vin_stg is null and cc_vehicle.LicensePlate_stg is null) then cc_vehicle.PublicID_stg 								

end ) as varchar(100)) as id 								

,''PRTY_ASSET_SBTYPE4''  as assettype ,''PRTY_ASSET_CLASFCN3'' as classification_code,''SRC_SYS6'' as src_cd								

from 								

DB_T_PROD_STAG.cc_vehicle 								

where   								

cc_vehicle.UpdateTime_stg > (:start_dttm)								

and cc_vehicle.UpdateTime_stg <= ( :end_dttm)								

and  policysystemid_stg is null								

								

UNION								

								

/** Real Estate **/								

/** Main Dwelling  **/								

select distinct cast(pcx_dwelling_hoe.fixedid_stg  as varchar(100))as id , ''PRTY_ASSET_SBTYPE5'' as assettype ,''PRTY_ASSET_CLASFCN1'' as classification_code,''SRC_SYS4'' as src_cd								

from 								

DB_T_PROD_STAG.pcx_dwelling_hoe 								

join DB_T_PROD_STAG.pcx_holocation_hoe 								

    on pcx_dwelling_hoe.holocation_stg = pcx_holocation_hoe.ID_stg								

where   pcx_dwelling_hoe.UpdateTime_stg>(:start_dttm) 								

AND pcx_dwelling_hoe.UpdateTime_stg <= ( :end_dttm)								

AND pcx_dwelling_hoe.fixedid_stg is not null								

								

UNION								

/**Dwelling Personal Property and Other Structure**/								

select 								

distinct								

cast(pcx_holineschedcovitem_alfa.FixedID_stg as varchar(100))as id								

,case when (pc_etlclausepattern.PatternID_stg like ''%ScheduledProperty%'' or pc_etlclausepattern.PatternID_stg like ''HOLI%'') then ''PRTY_ASSET_SBTYPE7''                                                                      																

    when (pc_etlclausepattern.PatternID_stg like ''%SpecificOtherStructure%'' or pc_etlclausepattern.PatternID_stg like ''HODW%'' or pc_etlclausepattern.PatternID_stg like ''HOSI%'')                                                                    																

    then ''PRTY_ASSET_SBTYPE5'' end as assettype                                                                  																

    ,case when assettype=''PRTY_ASSET_SBTYPE5'' THEN ''PRTY_ASSET_CLASFCN1'' else ChoiceTerm1_stg end as classification_code,''SRC_SYS4'' as src_cd								

from 								

DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa								

left outer join DB_T_PROD_STAG.pc_etlclausepattern 								

    on pc_etlclausepattern.PatternID_stg=pcx_holineschcovitemcov_alfa.PatternCode_stg								

left outer join DB_T_PROD_STAG.pcx_holineschedcovitem_alfa 								

    on pcx_holineschedcovitem_alfa.ID_stg=pcx_holineschcovitemcov_alfa.HOLineSchCovItem_stg								

where															

 pcx_holineschedcovitem_alfa.FixedID_stg is not null 								

and pcx_holineschcovitemcov_alfa.UpdateTime_stg > (:start_dttm) 								

and pcx_holineschcovitemcov_alfa.UpdateTime_stg <= ( :end_dttm)								

								

								

UNION								

/**Building and property**/								

								

select distinct cast(c.fixedid_stg as varchar(100)) as id, ''PRTY_ASSET_SBTYPE13'' as assettype, cp.TYPECODE_stg as classification_code, ''SRC_SYS4'' as src_cd								

FROM 								

DB_T_PROD_STAG.pcx_bp7classification c								

INNER JOIN (select b.*, rank() over (partition by b.FixedId_stg order by b.UpdateTime_stg desc) r from DB_T_PROD_STAG.pcx_bp7building b) b 								

        on c.Building_stg = b.FixedID_stg								

        and c.branchid_stg=b.branchid_stg								

        and b.r = 1     								

INNER JOIN DB_T_PROD_STAG.pc_building building  on building.id_stg = b.Building_stg        								

INNER JOIN DB_T_PROD_STAG.pctl_bp7classificationproperty cp on cp.ID_stg = c.bp7classpropertytype_stg								

join DB_T_PROD_STAG.pctl_bp7classdescription cdesc on c.bp7classdescription_stg = cdesc.ID_stg 								

INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg = b.BranchID_stg								

INNER JOIN DB_T_PROD_STAG.pc_policy p on p.id_stg = pp.PolicyID_stg 								

LEFT JOIN DB_T_PROD_STAG.pctl_rooftype rt on b.BP7RoofType_alfa_stg = rt.ID_stg								

INNER JOIN (select l.*, rank() over (partition by l.FixedId_stg order by l.UpdateTime_stg desc) r from DB_T_PROD_STAG.pcx_bp7location l) l 								

        on b.Location_stg = l.FixedID_stg								

        and l.r = 1								

LEFT JOIN DB_T_PROD_STAG.pc_addlinterestdetail aid on aid.BP7Building_stg = b.ID_stg								

LEFT JOIN DB_T_PROD_STAG.pctl_additionalinteresttype ait on aid.AdditionalInterestType_stg = ait.ID_stg								

LEFT JOIN DB_T_PROD_STAG.pctl_bp7constructiontype ctype on ctype.ID_stg = b.bp7constructiontype_stg								

LEFT JOIN (   SELECT ba.BP7Building_stg,ba.QuestionCode_stg,ba.BooleanAnswer_stg,MAX( ba.updatetime_stg) AS updatetime								

                     FROM DB_T_PROD_STAG.pcx_bp7buildinganswer_alfa ba								

                     WHERE ba.BooleanAnswer_stg is not null 								

                     GROUP BY ba.BP7Building_stg,ba.QuestionCode_stg,ba.BooleanAnswer_stg								

)ba on ba.BP7Building_stg = b.ID_stg								

LEFT JOIN DB_T_PROD_STAG.pctl_bp7coolingtype_alfa cta on cta.ID_stg = b.BP7PriCoolingType_alfa_stg								

LEFT JOIN DB_T_PROD_STAG.pctl_bp7heatingtype_alfa hta on hta.ID_stg = b.BP7PriHeatingType_alfa_stg								

LEFT JOIN DB_T_PROD_STAG.pc_territorycode tc on tc.BranchID_stg = pp.ID_stg								

INNER JOIN DB_T_PROD_STAG.pc_policyline pol on pol.BranchID_stg = pp.ID_stg								

LEFT JOIN DB_T_PROD_STAG.pctl_bp7policytype_alfa pta on pta.ID_stg = pol.BP7PolicyType_alfa_stg								

WHERE ((c.UPDATETIME_stg > (:start_dttm) AND c.UPDATETIME_stg <= ( :end_dttm))								

    OR (b.UPDATETIME_stg > (:start_dttm) AND b.UPDATETIME_stg <= ( :end_dttm))								

    OR (l.UPDATETIME_stg > (:start_dttm) AND l.UPDATETIME_stg <= ( :end_dttm)))								

AND c.fixedid_stg is not null 								

								

/*UNION								

								

select distinct cast(bs.fixedid_stg as varchar(100)) as id, ''PRTY_ASSET_SBTYPE14'' as assettype, 								

StringTerm1_stg as classification_code, 								

''SRC_SYS4'' as src_cd								

from 								

DB_T_PROD_STAG.pcx_bp7bldgschedcovitemcov bs								

WHERE bs.UpdateTime_stg > (:start_dttm) and bs.UpdateTime_stg <= ( :end_dttm)								

and bs.fixedid_stg is not null*//* - there is no worklet in DB_T_PROD_CORE.prod environment .only stage table exists. Commenting out 								 */
								

UNION								

								

 select cast(pcx_BP7BldgSchedCovItem.FixedID_stg as varchar(50)) fixedid,  ''PRTY_ASSET_SBTYPE23'' assettype ,''PRTY_ASSET_CLASFCN8''   as classification_code, ''SRC_SYS4'' as src_cd								

from       								

    DB_T_PROD_STAG.pcx_BP7BldgSchedCovItem 								

    join DB_T_PROD_STAG.pc_policyperiod 								

    on  pcx_bp7bldgschedcovitem.BranchID_stg=pc_policyperiod.ID_stg								

where    pcx_bp7bldgschedcovitem.UpdateTime_stg  > (:start_dttm) 								

    and  pcx_bp7bldgschedcovitem.UpdateTime_stg <= ( :end_dttm)								

     and pcx_BP7BldgSchedCovItem.FixedID_stg is not null								

								

UNION								

								

select cast(pcx_BP7BldgSchedexclItem.FixedID_stg as varchar(50)) fixedid,  ''PRTY_ASSET_SBTYPE25'' assettype ,''PRTY_ASSET_CLASFCN8''   as classification_code, ''SRC_SYS4'' as src_cd								

from    DB_T_PROD_STAG.pcx_BP7BldgSchedexclItem  								

 join DB_T_PROD_STAG.pc_policyperiod 								

    on pcx_bp7bldgschedexclitem.BranchID_stg=pc_policyperiod.ID_stg								

where    pcx_BP7BldgSchedexclItem.FixedID_stg is not null								

and pcx_bp7bldgschedexclitem.UpdateTime_stg > (:start_dttm) 								

    and pcx_bp7bldgschedexclitem.UpdateTime_stg<= ( :end_dttm)								

    								

UNION								

								

 select cast(pcx_bp7bldgschedconditem.FixedID_stg as varchar(50)) fixedid,  ''PRTY_ASSET_SBTYPE24'' assettype ,''PRTY_ASSET_CLASFCN8''   as classification_code, ''SRC_SYS4'' as src_cd								

from       								

DB_T_PROD_STAG.pcx_bp7bldgschedconditem join DB_T_PROD_STAG.pc_policyperiod 								

    on  pcx_bp7bldgschedconditem.BranchID_stg=pc_policyperiod.ID_stg								

where   pcx_bp7bldgschedconditem.UpdateTime_stg  > (:start_dttm) 								

    and  pcx_bp7bldgschedconditem.UpdateTime_stg <= ( :end_dttm)								

    and pcx_bp7bldgschedconditem.FixedID_stg is not null								

								

UNION								

								

 select cast(pcx_BP7ClassSchedCovItem.FixedID_stg as varchar(50)) fixedid,  ''PRTY_ASSET_SBTYPE26'' assettype ,''PRTY_ASSET_CLASFCN8''   as classification_code, ''SRC_SYS4'' as src_cd								

from    DB_T_PROD_STAG.pcx_BP7ClassSchedCovItem    								

    join DB_T_PROD_STAG.pc_policyperiod 								

    on pcx_bp7classschedcovitem.BranchID_stg= pc_policyperiod.ID_stg								

where    pcx_bp7classschedcovitem.UpdateTime_stg > (:start_dttm) 								

    and  pcx_bp7classschedcovitem.UpdateTime_stg<= ( :end_dttm)								

     and pcx_BP7ClassSchedCovItem.FixedID_stg is not null								

								

UNION								

								

 select cast(pcx_BP7ClassSchedexclItem.FixedID_stg as varchar(50)) fixedid,  ''PRTY_ASSET_SBTYPE28'' assettype ,''PRTY_ASSET_CLASFCN8''   as classification_code, ''SRC_SYS4'' as src_cd								

from    DB_T_PROD_STAG.pcx_BP7ClassSchedexclItem    								

join DB_T_PROD_STAG.pc_policyperiod 								

    on pcx_bp7classschedexclitem.BranchID_stg=pc_policyperiod.ID_stg								

where   pcx_bp7classschedexclitem.UpdateTime_stg > (:start_dttm) 								

    and pcx_bp7classschedexclitem.UpdateTime_stg <= ( :end_dttm)								

     and pcx_BP7ClassSchedexclItem.FixedID_stg is not null								

								

UNION								

								

select cast(pcx_BP7ClassSchedCondItem.FixedID_stg as varchar(50)) fixedid,  ''PRTY_ASSET_SBTYPE27'' assettype ,''PRTY_ASSET_CLASFCN8''   as classification_code, ''SRC_SYS4'' as src_cd								

from    DB_T_PROD_STAG.pcx_BP7ClassSchedCondItem 								

join DB_T_PROD_STAG.pc_policyperiod 								

    on pcx_bp7classschedconditem.BranchID_stg=pc_policyperiod.ID_stg								

where   pcx_bp7classschedconditem.UpdateTime_stg > (:start_dttm) 								

    and pcx_bp7classschedconditem.UpdateTime_stg<= ( :end_dttm)								

 and pcx_BP7ClassSchedCondItem.fixedid_stg is not null								

								

UNION								

								

select cast(pcx_bp7lineschedcovitem.FixedID_stg as varchar(50)) fixedid,  ''PRTY_ASSET_SBTYPE29'' assettype ,''PRTY_ASSET_CLASFCN8''   as classification_code, ''SRC_SYS4'' as src_cd								

from    								

DB_T_PROD_STAG.pcx_bp7lineschedcovitem join DB_T_PROD_STAG.pc_policyperiod 								

    on pcx_bp7lineschedcovitem.BranchID_stg=pc_policyperiod.ID_stg								

where   pcx_bp7lineschedcovitem.UpdateTime_stg > (:start_dttm) 								

    and pcx_bp7lineschedcovitem.UpdateTime_stg<= ( :end_dttm)								

  and pcx_bp7lineschedcovitem.fixedid_stg is not null								

								

UNION								

								

 select cast(pcx_bp7lineschedexclitem.FixedID_stg as varchar(50)) fixedid,  ''PRTY_ASSET_SBTYPE31'' assettype ,''PRTY_ASSET_CLASFCN8''   as classification_code, ''SRC_SYS4'' as src_cd								

from  								

 DB_T_PROD_STAG.pcx_bp7lineschedexclitem  join DB_T_PROD_STAG.pc_policyperiod 								

    on pcx_bp7lineschedexclitem.BranchID_stg=pc_policyperiod.ID_stg								

where   pcx_bp7lineschedexclitem.UpdateTime_stg > (:start_dttm) 								

    and pcx_bp7lineschedexclitem.UpdateTime_stg<= ( :end_dttm)								

 and pcx_bp7lineschedexclitem.fixedid_stg is not null								

								

UNION								

								

 select cast(pcx_bp7lineschedconditem.FixedID_stg as varchar(50)) fixedid,  ''PRTY_ASSET_SBTYPE30'' assettype ,''PRTY_ASSET_CLASFCN8''   as classification_code, ''SRC_SYS4'' as src_cd								

from								

DB_T_PROD_STAG.pcx_bp7lineschedconditem  join DB_T_PROD_STAG.pc_policyperiod 								

    on pcx_bp7lineschedconditem.BranchID_stg=pc_policyperiod.ID_stg								

where   pcx_bp7lineschedconditem.UpdateTime_stg > (:start_dttm) 								

    and pcx_bp7lineschedconditem.UpdateTime_stg<= ( :end_dttm)								

 and pcx_bp7lineschedconditem.fixedid_stg is not null								

								

UNION								

 select cast(pcx_BP7LocSchedCovItem.FixedID_stg as varchar(50)) fixedid,  ''PRTY_ASSET_SBTYPE20'' assettype ,''PRTY_ASSET_CLASFCN8''   as classification_code, ''SRC_SYS4'' as src_cd								

from    DB_T_PROD_STAG.pcx_BP7LocSchedCovItem    								

 join DB_T_PROD_STAG.pc_policyperiod 								

    on pcx_bp7locschedcovitem.BranchID_stg=pc_policyperiod.ID_stg								

where   pcx_bp7locschedcovitem.UpdateTime_stg > (:start_dttm) 								

    and pcx_bp7locschedcovitem.UpdateTime_stg <= ( :end_dttm)								

     and pcx_BP7LocSchedCovItem.fixedid_stg is not null								

								

UNION								

 select cast(pcx_BP7LocSchedexclItem.FixedID_stg as varchar(50)) fixedid,  ''PRTY_ASSET_SBTYPE22'' assettype ,''PRTY_ASSET_CLASFCN8''   as classification_code, ''SRC_SYS4'' as src_cd								

from    DB_T_PROD_STAG.pcx_BP7LocSchedexclItem  								

join DB_T_PROD_STAG.pc_policyperiod 								

    on pcx_bp7locschedexclitem.BranchID_stg=pc_policyperiod.ID_stg								

where   pcx_bp7locschedexclitem.UpdateTime_stg > (:start_dttm) 								

    and pcx_bp7locschedexclitem.UpdateTime_stg  <= ( :end_dttm)								

     and pcx_BP7LocSchedexclItem.fixedid_stg is not null								

								

								

UNION								

 select cast(pcx_BP7LocSchedCondItem.FixedID_stg as varchar(50)) fixedid,  ''PRTY_ASSET_SBTYPE21'' assettype ,''PRTY_ASSET_CLASFCN8''   as classification_code, ''SRC_SYS4'' as src_cd								

from    DB_T_PROD_STAG.pcx_BP7LocSchedCondItem 								

join DB_T_PROD_STAG.pc_policyperiod on pcx_bp7locschedconditem.BranchID_stg=pc_policyperiod.ID_stg 								

where  pcx_bp7locschedconditem.UpdateTime_stg > (:start_dttm) 								

and pcx_bp7locschedconditem.UpdateTime_stg <= ( :end_dttm)								

 and pcx_BP7LocSchedCondItem.fixedid_stg is not null								

								

								

         /** CLAIM PROPERTY AND OTHER **/								

UNION								

/** DWELLING **/								

/* DwellingIncident: Unverfied dwelling								 */
/* OtherStructureIncident: Unverfied/liability other structure such as pool, garage etc								 */
/* PropertyContentsIncident: Unverfied/liability property content								 */
/* FixedPropertyIncident: liability dwelling								 */
select 								

distinct								

cast(cc_incident.PublicID_stg as varchar(64)) as id, /*EIM-37477*/								

case when cctl_incident.name_stg = ''FixedPropertyIncident'' then ''PRTY_ASSET_SBTYPE5''								

when cctl_incident.name_stg =''OtherStructureIncident'' then ''PRTY_ASSET_SBTYPE11'' end as assettype , 								

case when cctl_incident.name_stg =''FixedPropertyIncident'' then ''PRTY_ASSET_CLASFCN1''								

when cctl_incident.name_stg = ''OtherStructureIncident'' then ''PRTY_ASSET_CLASFCN7'' end as classification_type ,								

''SRC_SYS6'' as src_cd								

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

cc_incident.UpdateTime_stg > (:start_dttm)								

and cc_incident.UpdateTime_stg <= ( :end_dttm)								

and cctl_incident.name_stg in (''FixedPropertyIncident'',''OtherStructureIncident'')								

								

UNION								

select 								

distinct								

cast(cc_incident.PublicID_stg as varchar(64)) as id, /*EIM-37477*/									

''PRTY_ASSET_SBTYPE5'' as assettype , 								

''PRTY_ASSET_CLASFCN1'' as classification_type , ''SRC_SYS6'' as src_cd								

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

cc_incident.UpdateTime_stg > (:start_dttm)								

and cc_incident.UpdateTime_stg <= ( :end_dttm)								

and cctl_incident.name_stg=''DwellingIncident''								

and cc_policylocation.PolicySystemId_stg is null								

								

UNION								

								

select 								

distinct								

cast(cc_assessmentcontentitem.PublicID_stg as varchar(64)) as id, 	/*EIM-37477*/								

''PRTY_ASSET_SBTYPE11'' as assettype , 								

cctl_contentlineitemschedule.TYPECODE_stg as classification_type , ''SRC_SYS6'' as src_cd								

from 								

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

left join DB_T_PROD_STAG.cctl_contentlineitemschedule 								

    on cc_assessmentcontentitem.ContentSchedule_stg = cctl_contentlineitemschedule.ID_stg								

where    cc_assessmentcontentitem.UpdateTime_stg > (:start_dttm) 								

AND cc_assessmentcontentitem.UpdateTime_stg <= ( :end_dttm)								

AND cctl_incident.name_stg=''PropertyContentsIncident'' 								

								

UNION								

								

/* BUILDING								 */
SELECT distinct								

    cast(a.fixedid_stg as varchar(100)) as id,								

    cast(''PRTY_ASSET_SBTYPE32'' as varchar(50)) as PRTY_ASSET_SBTYPE_CD,								

    cast(''PRTY_ASSET_CLASFCN10'' as varchar(50))as classification_code,								

    ''SRC_SYS4'' as src_cd 								

from DB_T_PROD_STAG.pcx_bp7building a								

join DB_T_PROD_STAG.pc_building b on b.FixedID_stg = a.Building_stg and b.BranchID_stg = a.BranchID_stg								

where  ((a.updatetime_stg > (:start_dttm)  AND a.updatetime_stg <= ( :end_dttm))								

    OR (b.updatetime_stg > (:start_dttm) AND b.updatetime_stg <= ( :end_dttm)))

	

union

/* fop dwelling */
select distinct cast(pcx_fopdwelling.fixedid_stg  as varchar(100))as id,

''PRTY_ASSET_SBTYPE37'' as assettype ,''PRTY_ASSET_CLASFCN15'' as classification_code,''SRC_SYS4'' as src_cd

from DB_T_PROD_STAG.pcx_fopdwelling

where  pcx_fopdwelling.fixedid_stg is not null

AND pcx_fopdwelling.UpdateTime_stg >(:start_dttm)

AND  pcx_fopdwelling.UpdateTime_stg <= ( :end_dttm)





union

/* fop outbuilding */
select distinct cast(pcx_fopoutbuilding.fixedid_stg  as varchar(100))as id ,

cast(''PRTY_ASSET_SBTYPE36'' as varchar(50)) as assettype ,

cast(''PRTY_ASSET_CLASFCN13'' as varchar(50)) as classification_code,''SRC_SYS4'' as src_cd

from DB_T_PROD_STAG.pcx_fopoutbuilding

where pcx_fopoutbuilding.fixedid_stg is not null

and pcx_fopoutbuilding.UpdateTime_stg >(:start_dttm) 

and pcx_fopoutbuilding.UpdateTime_stg <= ( :end_dttm)



union

/* fop feedandseed */
select distinct cast(pcx_fopfeedandseed.fixedid_stg  as varchar(100))as id ,

cast(''PRTY_ASSET_SBTYPE33'' as varchar(50)) as assettype ,

cast(''PRTY_ASSET_CLASFCN11'' as varchar(50)) as classification_code,''SRC_SYS4'' as src_cd

from DB_T_PROD_STAG.pcx_fopfeedandseed

where pcx_fopfeedandseed.fixedid_stg is not null

and pcx_fopfeedandseed.UpdateTime_stg >(:start_dttm) 

and pcx_fopfeedandseed.UpdateTime_stg <= ( :end_dttm)



union

/* fop livestock */
select distinct cast(pcx_foplivestock.fixedid_stg  as varchar(100))as id ,

cast(''PRTY_ASSET_SBTYPE35'' as varchar(50)) as assettype ,

cast(''PRTY_ASSET_CLASFCN14'' as varchar(50)) as classification_code,''SRC_SYS4'' as src_cd

from DB_T_PROD_STAG.pcx_foplivestock

where pcx_foplivestock.fixedid_stg is not null

and pcx_foplivestock.UpdateTime_stg >(:start_dttm) 

and pcx_foplivestock.UpdateTime_stg <= ( :end_dttm)



union

/* fop machinery */
select distinct cast(pcx_fopmachinery.fixedid_stg  as varchar(100))as id ,

cast(''PRTY_ASSET_SBTYPE34'' as varchar(50)) as assettype ,

cast(''PRTY_ASSET_CLASFCN12'' as varchar(50)) as classification_code,''SRC_SYS4'' as src_cd

from DB_T_PROD_STAG.pcx_fopmachinery

where pcx_fopmachinery.fixedid_stg is not null

and pcx_fopmachinery.UpdateTime_stg >(:start_dttm) 

and pcx_fopmachinery.UpdateTime_stg <= ( :end_dttm)



union

/* fop dwellingschdcovitem */
select distinct cast(pcx_fopdwellingschdcovitem.fixedid_stg  as varchar(100))as id ,

cast(''PRTY_ASSET_SBTYPE38'' as varchar(50)) as assettype ,

cast(''PRTY_ASSET_CLASFCN16'' as varchar(50)) as classification_code,''SRC_SYS4'' as src_cd

from DB_T_PROD_STAG.pcx_fopdwellingschdcovitem

where pcx_fopdwellingschdcovitem.fixedid_stg is not null

and pcx_fopdwellingschdcovitem.UpdateTime_stg >(:start_dttm) 

and pcx_fopdwellingschdcovitem.UpdateTime_stg <= ( :end_dttm)



union

/* fop dwellingschdexclitem */
select distinct cast(pcx_fopdwellingschdexclitem.fixedid_stg  as varchar(100))as id ,

cast(''PRTY_ASSET_SBTYPE40'' as varchar(50)) as assettype ,

cast(''PRTY_ASSET_CLASFCN18'' as varchar(50)) as classification_code,''SRC_SYS4'' as src_cd

from DB_T_PROD_STAG.pcx_fopdwellingschdexclitem

where pcx_fopdwellingschdexclitem.fixedid_stg is not null

and pcx_fopdwellingschdexclitem.UpdateTime_stg >(:start_dttm) 

and pcx_fopdwellingschdexclitem.UpdateTime_stg <= ( :end_dttm)



union



select distinct cast(pcx_fopfarmownerslischdcovitem.fixedid_stg  as varchar(100))as id ,

cast(''PRTY_ASSET_SBTYPE41'' as varchar(50)) as assettype ,

cast(''PRTY_ASSET_CLASFCN19'' as varchar(50)) as classification_code,''SRC_SYS4'' as src_cd

from DB_T_PROD_STAG.pcx_fopfarmownerslischdcovitem

where pcx_fopfarmownerslischdcovitem.fixedid_stg is not null

and pcx_fopfarmownerslischdcovitem.UpdateTime_stg >(:start_dttm) 

and pcx_fopfarmownerslischdcovitem.UpdateTime_stg <= ( :end_dttm)



union



select distinct cast(pcx_fopliabilityschdcovitem.fixedid_stg  as varchar(100))as id ,

cast(''PRTY_ASSET_SBTYPE42'' as varchar(50)) as assettype ,

cast(''PRTY_ASSET_CLASFCN20'' as varchar(50)) as classification_code,''SRC_SYS4'' as src_cd

from DB_T_PROD_STAG.pcx_fopliabilityschdcovitem

where pcx_fopliabilityschdcovitem.fixedid_stg is not null

and pcx_fopliabilityschdcovitem.UpdateTime_stg >(:start_dttm) 

and pcx_fopliabilityschdcovitem.UpdateTime_stg <= ( :end_dttm)

       						

) as TMP
) SRC
)
);


-- Component EXPTRANS, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXPTRANS AS
(
SELECT
SQ_XREF_PRTY_ASSET.ASSET_HOST_ID_VAL as ASSET_HOST_ID_VAL,
SQ_XREF_PRTY_ASSET.LOAD_DTTM as LOAD_DTTM,
LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE */ as PRTY_ASSET_SBTYPE_CD1,
LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN */ as PRTY_ASSET_CLASFCN_CD1,
LKP_3.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SRC_CD */ as SRC_SYS_CD1,
SQ_XREF_PRTY_ASSET.source_record_id,
row_number() over (partition by SQ_XREF_PRTY_ASSET.source_record_id order by SQ_XREF_PRTY_ASSET.source_record_id) as RNK
FROM
SQ_XREF_PRTY_ASSET
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = SQ_XREF_PRTY_ASSET.PRTY_ASSET_SBTYPE_CD
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = SQ_XREF_PRTY_ASSET.PRTY_ASSET_CLASFCN_CD
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_SRC_CD LKP_3 ON LKP_3.SRC_IDNTFTN_VAL = SQ_XREF_PRTY_ASSET.SRC_SYS_CD
QUALIFY RNK = 1
);


-- Component LKP_XREF_PRTY_ASSET, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_XREF_PRTY_ASSET AS
(
SELECT
LKP.PRTY_ASSET_ID,
LKP.ASSET_HOST_ID_VAL,
LKP.LOAD_DTTM,
EXPTRANS.source_record_id,
ROW_NUMBER() OVER(PARTITION BY EXPTRANS.source_record_id ORDER BY LKP.PRTY_ASSET_ID asc,LKP.PRTY_ASSET_SBTYPE_CD asc,LKP.ASSET_HOST_ID_VAL asc,LKP.PRTY_ASSET_CLASFCN_CD asc,LKP.SRC_SYS_CD asc,LKP.LOAD_DTTM asc) RNK
FROM
EXPTRANS
LEFT JOIN (
SELECT
PRTY_ASSET_ID,
PRTY_ASSET_SBTYPE_CD,
ASSET_HOST_ID_VAL,
PRTY_ASSET_CLASFCN_CD,
SRC_SYS_CD,
LOAD_DTTM
FROM DB_T_PROD_CORE.DIR_PRTY_ASSET
) LKP ON LKP.PRTY_ASSET_SBTYPE_CD = EXPTRANS.PRTY_ASSET_SBTYPE_CD1 AND LKP.ASSET_HOST_ID_VAL = EXPTRANS.ASSET_HOST_ID_VAL AND LKP.PRTY_ASSET_CLASFCN_CD = EXPTRANS.PRTY_ASSET_CLASFCN_CD1
QUALIFY RNK = 1
);


-- Component fil_xref_prty_asset, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE fil_xref_prty_asset AS
(
SELECT
LKP_XREF_PRTY_ASSET.PRTY_ASSET_ID as PRTY_ASSET_ID,
EXPTRANS.PRTY_ASSET_SBTYPE_CD1 as PRTY_ASSET_SBTYPE_CD,
EXPTRANS.ASSET_HOST_ID_VAL as ASSET_HOST_ID_VAL,
EXPTRANS.PRTY_ASSET_CLASFCN_CD1 as PRTY_ASSET_CLASFCN_CD,
EXPTRANS.SRC_SYS_CD1 as SRC_SYS_CD,
EXPTRANS.LOAD_DTTM as LOAD_DTTM,
EXPTRANS.source_record_id
FROM
EXPTRANS
LEFT JOIN LKP_XREF_PRTY_ASSET ON EXPTRANS.source_record_id = LKP_XREF_PRTY_ASSET.source_record_id
WHERE LKP_XREF_PRTY_ASSET.PRTY_ASSET_ID IS NULL
);


-- Component DIR_PRTY_ASSET, Type TARGET 
INSERT INTO DB_T_PROD_CORE.DIR_PRTY_ASSET
(
PRTY_ASSET_ID,
PRTY_ASSET_SBTYPE_CD,
ASSET_HOST_ID_VAL,
PRTY_ASSET_CLASFCN_CD,
SRC_SYS_CD,
LOAD_DTTM
)
SELECT
row_number() over (order by 1) as PRTY_ASSET_ID,
fil_xref_prty_asset.PRTY_ASSET_SBTYPE_CD as PRTY_ASSET_SBTYPE_CD,
fil_xref_prty_asset.ASSET_HOST_ID_VAL as ASSET_HOST_ID_VAL,
fil_xref_prty_asset.PRTY_ASSET_CLASFCN_CD as PRTY_ASSET_CLASFCN_CD,
fil_xref_prty_asset.SRC_SYS_CD as SRC_SYS_CD,
fil_xref_prty_asset.LOAD_DTTM as LOAD_DTTM
FROM
fil_xref_prty_asset;


END; ';