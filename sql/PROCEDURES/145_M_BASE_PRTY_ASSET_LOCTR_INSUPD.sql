-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_PRTY_ASSET_LOCTR_INSUPD("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '  

-- PIPELINE START FOR 1
declare
start_dttm timestamp;
end_dttm timestamp;
prcs_id integer;
BEGIN
start_dttm := current_timestamp();
end_dttm := current_timestamp();
prcs_id :=1;


-- Component SQ_prty_asset_loctr_x, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_prty_asset_loctr_x AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as SRC_PRTY_ASSET_ID,
$2 as SRC_LOC_ID,
$3 as SRC_LOCATOR_ROLE_CD,
$4 as SRC_CODE,
$5 as SRC_START_DT,
$6 as SRC_PRTY_ASSET_LOCTR_END_DTTM,
$7 as SRC_FIRE_DEPT_ID,
$8 as SRC_FIRE_DEPT_OUT_OF_CNTY_IND,
$9 as SRC_LOC_ROLE_CD,
$10 as SRC_TRANS_STRT_DTTM,
$11 as SRC_PRTY_ASSET_LOCTR_ROLE,
$12 as RANKINDEX,
$13 as EDW_STRT_DTTM,
$14 as EDW_END_DTTM,
$15 as TGT_LOC_ID,
$16 as TGT_PRTY_ASSET_LOCTR_STRT_DTTM,
$17 as TGT_PRTY_ASSET_LOCTR_END_DTTM,
$18 as TGT_FIRE_DEPT_ID,
$19 as TGT_PRTY_ASSET_ID,
$20 as TGT_PRTY_ASSET_LOCTR_ROLE_CD,
$21 as TARGETDATA,
$22 as SOURCEDATA,
$23 as FLAG,
$24 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
WITH LOC AS ( 

	SELECT	distinct SRC.addressline1,SRC.addressline2,SRC.addressline3,

						SRC.typecode,SRC.AssetKey, SRC.AssetType,SRC.ClassificationCD,

						SRC.Mfg_Home_PrkCD,SRC.NK_Fire_Dept_ID,FIRE_DEPT_OUT_OF_CNTY_IND,

			SRC.City_Name, SRC.County_Name,SRC.StateTypecode,SRC.Country,

			SRC.Locator_RoleCD,SRC.strtdt,SRC.SourceCD, SRC.trans_strtdt,

			ASSET_SBTYPE.PRTY_ASSET_SBTYPE_CD,ASSET_CLASFN.ASSET_CLASFCN_CD,

			CTRY.ctry_ID, TERR.TERR_ID AS LOC_ID_in1,CNTY.CNTY_ID AS LOC_ID_in2,

			CITY.CITY_ID AS LOC_ID_in3, lkp_street_addr.STREET_ADDR_ID AS LOC_ID_in4,

			POSTL_CD.POSTL_CD_ID AS LOC_ID_in5, SRC_CD.TGT_IDNTFTN_VAL AS SOURCE,

			FIRE_DEPT.FIRE_DEPT_ID,PRTY_ASSET.PRTY_ASSET_ID 

	FROM	( /* ********************************************************************************** SQ Query*/ 

	SELECT	distinct addressline1,addressline2,addressline3,typecode,

			AssetKey, AssetType, ClassificationCD, Mfg_Home_PrkCD, NK_Fire_Dept_ID,

			FIRE_DEPT_OUT_OF_CNTY_IND, City_Name, County_Name, StateTypecode,

			Country, Locator_RoleCD, strtdt, SourceCD , trans_strtdt 

	from	( 

		SELECT	distinct addressline1,addressline2,addressline3,postalcode as typecode,

				fixedid as AssetKey, AssetType, ClassificationCD, Mfg_Home_PrkCD,

				NK_Fire_Dept_ID, FIRE_DEPT_OUT_OF_CNTY_IND, City_Name, County_Name,

				StateTypecode, Country, Locator_RoleCD, 

				case 

					when strtdt is null then cast(''1900-01-01 00:00:00.000000'' as timestamp(6)) 

					else strtdt 

				end as strtdt, src_cd as SourceCD, 

				case 

					when trans_strtdt is null then current_timestamp 

					else trans_strtdt 

				end as trans_strtdt 

		from	( 





 /* *****************************GARAGE LOCATION ************************** */ 

			select	distinct cast(null as varchar(100)) as addressline1, cast(null as varchar(100)) as addressline2,

					cast(null as varchar(100)) as addressline3, cast(null as varchar(100)) as postalcode,

					case 

when ccv.PolicySystemId_stg is not null then /*
SUBSTRING(ccv.policysystemid_stg,charindex('':'',ccv.policysystemid_stg)+1,LEN(ccv.policysystemid_stg))*/
substr(ccv.PolicySystemId_stg, 
					position('':'' in ccv.PolicySystemId_stg)+1,length(ccv.PolicySystemId_stg)-position('':'' in ccv.PolicySystemId_stg)) 

						when (ccv.PolicySystemId_stg is null 

				and ccv.Vin_stg is not null) then (''VIN:''||ccv.vin_stg) 

						when (ccv.PolicySystemId_stg is null 

				and ccv.Vin_stg is null 

				and ccv.LicensePlate_stg is not null) then (''LP:''||ccv.licenseplate_stg) 

						when (ccv.PolicySystemId_stg is null 

				and ccv.Vin_stg is null 

				and ccv.LicensePlate_stg is null) then ccv.PublicID_stg 

					end as fixedid , CAST(''PRTY_ASSET_SBTYPE4'' as varchar(60)) AS assettype, CAST(''PRTY_ASSET_CLASFCN3'' as varchar(60)) as ClassificationCD,

					CAST(''VEHICLE'' as varchar(60)) as Mfg_Home_PrkCD, cast(null as varchar(100)) as NK_Fire_Dept_ID,

					cast(null as varchar(3)) as FIRE_DEPT_OUT_OF_CNTY_IND, cast(null as varchar(100)) as City_Name,

					cast(null as varchar(100)) as County_Name,ccs.TYPECODE_stg AS StateTypecode,

ccc.typecode_stg as Country, CAST(''CLAIM'' as varchar(60)) as Locator_RoleCD, ccv.createtime_stg as strtdt,/* EIM-47230 CLMST */
					''SRC_SYS6'' as src_cd, ccv.UpdateTime_stg as trans_strtdt  

			from	DB_T_PROD_STAG.cc_vehicle ccv 

			left outer join DB_T_PROD_STAG.cc_incident ci 

				on ci.vehicleid_stg=ccv.id_stg 

			left outer join DB_T_PROD_STAG.cc_claim cc 

				on cc.id_stg=ci.claimid_stg 

			left outer join DB_T_PROD_STAG.cc_address ca 

				on cc.LossLocationID_stg = ca.ID_stg 

			left outer JOIN DB_T_PROD_STAG.cctl_state ccs 

				ON ca.state_stg= ccs.id_stg 

			inner JOIN DB_T_PROD_STAG.cctl_country ccc 

ON ccc.id_stg = ca.country_stg 
/* left outer join DB_T_PROD_STAG.cctl_jurisdiction on cc_vehicle.state=cctl_jurisdiction.id*/ 
			where	ccv.UpdateTime_stg>(:START_DTTM) 

				AND ccv.UpdateTime_stg <= (:end_dttm) /******************************RISK LOCATION ***************************/ 

			

			

			union all /***** Dwelling Risk Location ********************************/ 

			

			SELECT	distinct cast(pl.addressline1Internal_stg as varchar(100)) as addressline1,

					cast(pl.addressline2Internal_stg as varchar(100)) as addressline2,

					cast(pl.addressline3Internal_stg as varchar(100)) as addressline3,

					cast(pl.postalcodeinternal_stg as varchar(100)) as postalcode,

					cast(pdh.fixedid_stg as varchar(100)) as fixedid, ''PRTY_ASSET_SBTYPE5'' as assettype ,

					''PRTY_ASSET_CLASFCN1'' as ClassificationCD, ''DWELLING'' as Mfg_Home_PrkCD,

					pfa.PublicID_stg as NK_Fire_Dept_ID , cast(phh.isfiredeptinothercounty_alfa_stg as varchar(3)) FIRE_DEPT_OUT_OF_CNTY_IND,

					pl.cityinternal_stg as city_name, pl.countyinternal_stg as County_Name,

					upper(ps.TYPECODE_stg) as StateTypecode, upper(pc.TYPECODE_stg) as Country,

					''RISK'' as Locator_RoleCD,pp.EditEffectiveDate_stg as strtdt,

					''SRC_SYS4'' as src_cd, pdh.UpdateTime_stg as trans_strtdt 

FROM    DB_T_PROD_STAG.pcx_dwelling_hoe pdh /* -EIM-46218 */
            INNER JOIN DB_T_PROD_STAG.pcx_holocation_hoe phh 

                ON pdh.holocation_stg = phh.Fixedid_stg

            INNER JOIN DB_T_PROD_STAG.pc_policylocation pl 

                  ON phh.policylocation_stg = pl.id_stg

            inner join DB_T_PROD_STAG.pc_address pa 

                on pl.AccountLocation_stg = pa.ID_stg 

            join DB_T_PROD_STAG.pctl_state ps 

                on ps.id_stg=pa.State_stg 

            join DB_T_PROD_STAG.pctl_country pc 

                on pc.id_stg=pa.Country_stg 

            left outer Join DB_T_PROD_STAG.pcx_firedepartment_alfa pfa 

                on pfa.ID_stg=phh.Firedepartment_alfa_stg 

                INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp 

                on pdh.branchid_stg=pp.id_stg

                and phh.BranchID_stg = pp.ID_stg

                and (pdh.ExpirationDate_stg is NULL 

                or pdh.ExpirationDate_stg > pp.EditEffectiveDate_stg)

               and (phh.ExpirationDate_stg is NULL 

                or phh.ExpirationDate_stg > pp.EditEffectiveDate_stg) 

			WHERE	pdh.UpdateTime_stg>(:START_DTTM) 

				AND pdh.UpdateTime_stg <= (:end_dttm) 

			UNION ALL /***** DB_T_CORE_PROD.vehicle Risk Location ********************************/ 

			select	distinct pa.addressline1_stg as addressline1, pa.addressline2_stg as addressline2,

					pa.addressline3_stg as addressline3, pa.postalcode_stg as postalcode,

					cast(pv.fixedid_stg as varchar(100)) as fixedid ,''PRTY_ASSET_SBTYPE4'' as assettype ,

					''PRTY_ASSET_CLASFCN3'' as ClassificationCD, ''VEHICLE'' as Mfg_Home_PrkCD,

					cast(null as varchar(100)) as NK_Fire_Dept_ID, cast(null as varchar(3)) as FIRE_DEPT_OUT_OF_CNTY_IND,

					pa.city_stg as city_name, pa.county_stg as County_Name, upper(ps.TYPECODE_stg) as StateTypecode,

					upper(pc.TYPECODE_stg) as Country, ''RISK'' as Locator_RoleCD,

					pp.EditEffectiveDate_stg as strtdt, ''SRC_SYS4'' as src_cd, pv.UpdateTime_stg as trans_strtdt 

			from	DB_T_PROD_STAG.pc_personalvehicle pv join DB_T_PROD_STAG.pc_policylocation pl 

				on pv.garagelocation_stg = pl.id_stg join DB_T_PROD_STAG.pc_address pa 

				on pl.AccountLocation_stg=pa.ID_stg join DB_T_PROD_STAG.pctl_state ps 

				on ps.id_stg=pa.State_stg join DB_T_PROD_STAG.pctl_country pc 

				on pc.id_stg=pa.Country_stg join DB_T_PROD_STAG.pc_policyperiod pp 

				on pv.branchid_stg=pp.id_stg 

			where	pa.addressline1_stg is not null 

				and pv.ExpirationDate_stg is null 

				and pv.UpdateTime_stg>(:START_DTTM) 

				AND pv.UpdateTime_stg <= (:end_dttm) 

			UNION ALL /***** DB_T_CORE_PROD.vehicle Garage Location ********************************/ 

			select	distinct pa.addressline1_stg as addressline1, pa.addressline2_stg as addressline2,

					pa.addressline3_stg as addressline3, pa.postalcode_stg as postalcode,

					cast(pv.fixedid_stg as varchar(100)) as fixedid, ''PRTY_ASSET_SBTYPE4'' as assettype ,

					''PRTY_ASSET_CLASFCN3'' as ClassificationCD, ''VEHICLE'' as Mfg_Home_PrkCD,

					cast(null as varchar(100)) as NK_Fire_Dept_ID, cast(null as varchar(3)) as FIRE_DEPT_OUT_OF_CNTY_IND,

					pa.city_stg as city_name, pa.county_stg as County_Name, upper(ps.TYPECODE_stg) as StateTypecode,

					upper(pc.TYPECODE_stg) as Country, ''GARAGE'' as Locator_RoleCD,

					pp.EditEffectiveDate_stg as strtdt, ''SRC_SYS4'' as src_cd, pv.UpdateTime_stg as trans_strtdt 

			from	DB_T_PROD_STAG.pc_personalvehicle pv join DB_T_PROD_STAG.pc_policylocation pl 

				on pv.garagelocation_stg = pl.id_stg join DB_T_PROD_STAG.pc_address pa 

				on pl.AccountLocation_stg=pa.ID_stg join DB_T_PROD_STAG.pctl_state ps 

				on ps.id_stg=pa.State_stg join DB_T_PROD_STAG.pctl_country pc 

				on pc.id_stg=pa.Country_stg join DB_T_PROD_STAG.pc_policyperiod pp 

				on pv.branchid_stg=pp.id_stg 

			where	pa.addressline1_stg is not null 

				and pv.ExpirationDate_stg is null 

				and pv.UpdateTime_stg>(:START_DTTM) 

				AND pv.UpdateTime_stg <= (:end_dttm) 

UNION ALL 
/*  Dwelling Personal Property and Other Structure - RSKLCTN (defect:- 11042)*/  
			select	distinct pa.addressline1_stg as addressline1, pa.addressline2_stg as addressline2,

					pa.addressline3_stg as addressline3, pa.postalcode_stg as postalcode,

					cast(phc.FixedID_stg as varchar(100)) as fixedid, 

					case 

						when pe.PatternID_stg in (''HOSI_SpecificOtherStructureItem_alfa'',

					''HOSI_SpecificOtherStructureExclItem_alfa'') then ''PRTY_ASSET_SBTYPE5'' 

						when pe.PatternID_stg=''HOSI_ScheduledPropertyItem_alfa'' then ''PRTY_ASSET_SBTYPE7'' /*''REALSP-PP''*/ 

					end as assettype , ChoiceTerm1_stg as ClassificationCD, ''PERSONAL'' as Mfg_Home_PrkCD,

					cast(null as varchar(100)) as NK_Fire_Dept_ID, cast(phh.isfiredeptinothercounty_alfa_stg as varchar(10)) FIRE_DEPT_OUT_OF_CNTY_IND,

					pa.city_stg as city_name, pa.county_stg as County_Name, upper(ps.TYPECODE_stg) as StateTypecode,

					upper(pc.TYPECODE_stg) as Country, ''RISK'' as Locator_RoleCD,

					pp.EditEffectiveDate_stg as strtdt, ''SRC_SYS4'' as src_cd, pdh.UpdateTime_stg as trans_strtdt 

			from	DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa phi 

			inner join DB_T_PROD_STAG.pcx_holineschedcovitem_alfa phc 

				on phc.ID_stg=phi.HOLineSchCovItem_stg 

			inner join DB_T_PROD_STAG.pc_etlclausepattern pe 

				on pe.PatternID_stg=phi.PatternCode_stg 

			inner join DB_T_PROD_STAG.pc_policyperiod pp 

				on phi.BranchID_stg=pp.ID_stg 

			inner join DB_T_PROD_STAG.pcx_dwelling_hoe pdh 

				on pp.id_stg = pdh.branchid_stg 

			inner join DB_T_PROD_STAG.pcx_holocation_hoe phh 

				on pdh.HOLocation_stg = phh.id_stg 

			inner join DB_T_PROD_STAG.pc_policylocation pl 

				on phh.PolicyLocation_stg = pl.id_stg 

			inner join DB_T_PROD_STAG.pc_address pa 

				on pl.AccountLocation_stg = pa.ID_stg 

			inner join DB_T_PROD_STAG.pctl_state ps 

				on ps.id_stg=pa.State_stg 

			inner join DB_T_PROD_STAG.pctl_country pc 

				on pc.id_stg=pa.Country_stg 

			where	pe.PatternID_stg in (''HOSI_SpecificOtherStructureItem_alfa'',

					''HOSI_ScheduledPropertyItem_alfa'',''HOSI_SpecificOtherStructureExclItem_alfa'') 

				and phc.UpdateTime_stg>(:START_DTTM) 

				AND phc.UpdateTime_stg <= (:end_dttm) 

			union all /***** Dwelling Risk Location ********************************/ 

			SELECT	distinct pa.addressline1_stg as addressline1, pa.addressline2_stg as addressline2,

					pa.addressline3_stg as addressline3, pa.postalcode_stg as postalcode,

					cast(pdh.fixedid_stg as varchar(100)) as fixedid,''PRTY_ASSET_SBTYPE5'' as assettype ,

					''PRTY_ASSET_CLASFCN1'' as ClassificationCD, ''DWELLING'' as Mfg_Home_PrkCD,

					pfa.PublicID_stg as NK_Fire_Dept_ID ,cast(phh.isfiredeptinothercounty_alfa_stg as varchar(3)) FIRE_DEPT_OUT_OF_CNTY_IND,

					pa.city_stg as city_name, pa.county_stg as County_Name, upper(ps.TYPECODE_stg) as StateTypecode,

					upper(pc.TYPECODE_stg) as Country, ''PHYSICAL'' as Locator_RoleCD,

					pp.EditEffectiveDate_stg as strtdt, ''SRC_SYS4'' as src_cd, pdh.UpdateTime_stg as trans_strtdt 

FROM    DB_T_PROD_STAG.pcx_dwelling_hoe pdh /* --EIM-46218 */
            INNER JOIN DB_T_PROD_STAG.pcx_holocation_hoe phh 

                ON pdh.holocation_stg = phh.Fixedid_stg

            INNER JOIN DB_T_PROD_STAG.pc_policylocation pl 

                  ON phh.policylocation_stg = pl.id_stg

            inner join DB_T_PROD_STAG.pc_address pa 

                on pl.AccountLocation_stg = pa.ID_stg 

            join DB_T_PROD_STAG.pctl_state ps 

                on ps.id_stg=pa.State_stg 

            join DB_T_PROD_STAG.pctl_country pc 

                on pc.id_stg=pa.Country_stg 

            left outer Join DB_T_PROD_STAG.pcx_firedepartment_alfa pfa 

                on pfa.ID_stg=phh.Firedepartment_alfa_stg 

                INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp 

                on pdh.branchid_stg=pp.id_stg

                and phh.BranchID_stg = pp.ID_stg

                and (pdh.ExpirationDate_stg is NULL 

                or pdh.ExpirationDate_stg > pp.EditEffectiveDate_stg)

               and (phh.ExpirationDate_stg is NULL 

                or phh.ExpirationDate_stg > pp.EditEffectiveDate_stg) 

			WHERE	pdh.UpdateTime_stg>(:START_DTTM) 

				AND pdh.UpdateTime_stg <= (:end_dttm) 

			UNION ALL /********Building Property ************/ 

			select	distinct pa.addressline1_stg as addressline1, pa.addressline2_stg as addressline2,

					pa.addressline3_stg as addressline3, pa.postalcode_stg as postalcode,

					cast(bci.FixedID_stg as varchar(100)) as fixedid , ''PRTY_ASSET_SBTYPE14'' as assettype,

					StringTerm1_stg as ClassificationCD, ''BUILDING'' as Mfg_Home_PrkCD,

					cast(null as varchar(100)) as NK_Fire_Dept_ID, cast(null as varchar(3)) as FIRE_DEPT_OUT_OF_CNTY_IND,

					pa.city_stg as city_name, pa.county_stg as County_Name, upper(ps.TYPECODE_stg) as StateTypecode,

					upper(pc.TYPECODE_stg) as Country, ''RISK'' as Locator_RoleCD,

					pp.EditEffectiveDate_stg as strtdt, ''SRC_SYS4'' as src_cd, pbb.UpdateTime_stg as trans_strtdt 

			from	DB_T_PROD_STAG.pcx_bp7bldgschedcovitemcov bic 

			inner join DB_T_PROD_STAG.pcx_bp7bldgschedcovitem bci 

				on bci.ID_stg = bic.BldgSchedCovItem_stg 

			inner join DB_T_PROD_STAG.pc_etlclausepattern pe 

				on pe.PatternID_stg = bic.PatternCode_stg 

			inner join DB_T_PROD_STAG.pc_policyperiod pp 

				on bic.BranchID_stg = pp.ID_stg 

			inner join DB_T_PROD_STAG.pcx_bp7building pbb 

				on pp.ID_stg = pbb.BranchID_stg 

			inner join DB_T_PROD_STAG.pcx_bp7location pbl 

				on pbb.Location_stg = pbl.ID_stg 

			inner join DB_T_PROD_STAG.pc_policylocation pl 

				on pbl.Location_stg = pl.ID_stg 

			inner join DB_T_PROD_STAG.pc_address pa 

				on pl.AccountLocation_stg = pa.ID_stg 

			inner join DB_T_PROD_STAG.pctl_state ps 

				on ps.id_stg = pa.State_stg 

			inner join DB_T_PROD_STAG.pctl_country pc 

				on pc.ID_stg = pa.Country_stg 

			where	pe.PatternID_stg = ''BP7LossPayableItem'' 

				and bci.UpdateTime_stg > (:START_DTTM) 

				and bci.UpdateTime_stg <= (:end_dttm) 

			UNION ALL /********Building Risk Location ************/ 

			SELECT	distinct pa.addressline1_stg as addressline1, pa.addressline2_stg as addressline2,

					pa.addressline3_stg as addressline3, pa.postalcode_stg as postalcode,

					cast(pbc.FixedID_stg as varchar(100)) as fixedid, ''PRTY_ASSET_SBTYPE13'' as assettype,

					pbp.TYPECODE_stg as ClassificationCD, ''BUILDING'' as Mfg_Home_PrkCD,

					pfa.PublicID_stg as NK_Fire_Dept_ID, cast(null as varchar(3)) as FIRE_DEPT_OUT_OF_CNTY_IND,

					pa.city_stg as city_name, pa.county_stg as County_Name, upper(ps.TYPECODE_stg) as StateTypecode,

					upper(pc.TYPECODE_stg) as Country, ''RISK'' as Locator_RoleCD,

					pp.EditEffectiveDate_stg as strtdt, ''SRC_SYS4'' as src_cd, pbb.UpdateTime_stg as trans_strtdt 

			FROM	DB_T_PROD_STAG.pcx_bp7classification pbc 

			INNER JOIN ( 

				select	FixedID_stg, UpdateTime_stg, Location_stg, BranchID_stg,

						ExpirationDate_stg, rank() over ( 

				partition by FixedID_stg 

				order by UpdateTime_stg desc) r 

				from	db_t_prod_stag.pcx_bp7building) pbb 

				ON pbb.FixedID_stg = pbc.Building_stg 

				AND pbb.r = 1 

			INNER JOIN DB_T_PROD_STAG.pctl_bp7classificationproperty pbp 

				ON pbc.bp7classpropertytype_stg = pbp.ID_stg 

			INNER JOIN ( 

				select	FixedID_stg, UpdateTime_stg, Location_stg, FireDepartment_alfa_stg,

						rank() over ( 

				partition by FixedID_stg 

				order by UpdateTime_stg desc) r 

				from	db_t_prod_stag.pcx_bp7location) pl 

				ON pbb.Location_stg = pl.FixedID_stg 

				AND pl.r = 1 

			INNER JOIN DB_T_PROD_STAG.pc_policylocation ppl 

				ON pl.Location_stg = ppl.ID_stg 

			INNER JOIN DB_T_PROD_STAG.pc_address pa 

				ON ppl.AccountLocation_stg = pa.ID_stg 

			INNER JOIN DB_T_PROD_STAG.pctl_state ps 

				ON ps.ID_stg = pa.State_stg 

			INNER JOIN DB_T_PROD_STAG.pctl_country pc 

				ON pc.ID_stg = pa.Country_stg 

			LEFT OUTER JOIN DB_T_PROD_STAG.pcx_firedepartment_alfa pfa 

				ON pfa.ID_stg = pl.FireDepartment_alfa_stg 

			INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp 

				ON pbb.BranchID_stg = pp.ID_stg 

				and (pbb.ExpirationDate_stg is NULL 

or pbb.ExpirationDate_stg > pp.EditEffectiveDate_stg) 
/*  added for EIM-33403*/ 
			WHERE	pbc.UpdateTime_stg > (:start_dttm) 

				AND pbc.UpdateTime_stg <= (:end_dttm) 

			UNION ALL /********Building Physical Location ************/ 

			SELECT	distinct pa.addressline1_stg as addressline1, pa.addressline2_stg as addressline2,

					pa.addressline3_stg as addressline3, pa.postalcode_stg as postalcode,

					cast(pbc.FixedID_stg as varchar(100)) as fixedid, ''PRTY_ASSET_SBTYPE13'' as assettype,

					pbp.TYPECODE_stg as ClassificationCD, ''BUILDING'' as Mfg_Home_PrkCD,

					pfa.PublicID_stg as NK_Fire_Dept_ID, cast(null as varchar(3)) as FIRE_DEPT_OUT_OF_CNTY_IND,

					pa.city_stg as city_name, pa.county_stg as County_Name, upper(ps.TYPECODE_stg) as StateTypecode,

					upper(pc.TYPECODE_stg) as Country, ''PHYSICAL'' as Locator_RoleCD,

					pp.EditEffectiveDate_stg as strtdt, ''SRC_SYS4'' as src_cd, pbb.UpdateTime_stg as trans_strtdt 

			FROM	DB_T_PROD_STAG.pcx_bp7classification pbc 

			INNER JOIN ( 

				select	FixedID_stg, UpdateTime_stg, Location_stg, BranchID_stg,

						ExpirationDate_stg, rank() over ( 

				partition by FixedID_stg 

				order by UpdateTime_stg desc) r 

				from	db_t_prod_stag.pcx_bp7building) pbb 

				ON pbb.FixedID_stg = pbc.Building_stg 

				AND pbb.r = 1 

			INNER JOIN DB_T_PROD_STAG.pctl_bp7classificationproperty pbp 

				ON pbc.bp7classpropertytype_stg = pbp.ID_stg 

			INNER JOIN ( 

				select	FixedID_stg, UpdateTime_stg, Location_stg, FireDepartment_alfa_stg,

						rank() over ( 

				partition by FixedID_stg 

				order by UpdateTime_stg desc) r 

				from	db_t_prod_stag.pcx_bp7location) pbl 

				ON pbb.Location_stg = pbl.FixedID_stg 

				AND pbl.r = 1 

			INNER JOIN DB_T_PROD_STAG.pc_policylocation pl 

				ON pbl.Location_stg = pl.ID_stg 

			INNER JOIN DB_T_PROD_STAG.pc_address pa 

				ON pl.AccountLocation_stg = pa.ID_stg 

			INNER JOIN DB_T_PROD_STAG.pctl_state ps 

				ON ps.ID_stg = pa.State_stg 

			INNER JOIN DB_T_PROD_STAG.pctl_country pc 

				ON pc.ID_stg = pa.Country_stg 

			LEFT OUTER JOIN DB_T_PROD_STAG.pcx_firedepartment_alfa pfa 

				ON pfa.ID_stg = pbl.FireDepartment_alfa_stg 

			INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp 

				ON pbb.BranchID_stg = pp.ID_stg 

				and (pbb.ExpirationDate_stg is NULL 

or pbb.ExpirationDate_stg > pp.EditEffectiveDate_stg) /*  added for EIM-33403*/  
			WHERE	pbc.UpdateTime_stg > (:start_dttm) 

				AND pbc.UpdateTime_stg <= (:end_dttm) 

			UNION ALL /********Building RISK Location EIM-40222************/ 

			SELECT	distinct pa.addressline1_stg as addressline1, pa.addressline2_stg as addressline2,

					pa.addressline3_stg as addressline3, pa.postalcode_stg as postalcode,

					cast(a.FixedID_stg as varchar(100)) as fixedid, ''PRTY_ASSET_SBTYPE32'' as assettype,

					''PRTY_ASSET_CLASFCN10'' as classificationcd, ''BUILDING'' as Mfg_Home_PrkCD,

					pfa.PublicID_stg as NK_Fire_Dept_ID, cast(null as varchar(3)) as FIRE_DEPT_OUT_OF_CNTY_IND,

					pa.city_stg as city_name, pa.county_stg as County_Name, upper(ps.TYPECODE_stg) as StateTypecode,

					upper(pc.TYPECODE_stg) as Country, ''RISK'' as Locator_RoleCD,

					pp.EditEffectiveDate_stg as strtdt, ''SRC_SYS4'' as src_cd, a.UpdateTime_stg as trans_strtdt 

			from	DB_T_PROD_STAG.pcx_bp7building a 

			INNER JOIN DB_T_PROD_STAG.pc_building b 

				on b.FixedID_stg = a.Building_stg 

				and b.BranchID_stg = a.BranchID_stg 

			INNER JOIN ( 

				select	FixedID_stg, UpdateTime_stg, Location_stg, FireDepartment_alfa_stg,

						Expirationdate_stg, rank() over ( 

				partition by FixedID_stg 

				order by UpdateTime_stg desc) r 

				from	db_t_prod_stag.pcx_bp7location) pl 

				ON a.Location_stg = pl.FixedID_stg 

				AND pl.r = 1 

			INNER JOIN DB_T_PROD_STAG.pc_policylocation ppl 

				ON a.Location_stg = ppl.ID_stg 

			INNER JOIN DB_T_PROD_STAG.pc_address pa 

				ON ppl.AccountLocation_stg = pa.ID_stg 

			INNER JOIN DB_T_PROD_STAG.pctl_state ps 

				ON ps.ID_stg = pa.State_stg 

			INNER JOIN DB_T_PROD_STAG.pctl_country pc 

				ON pc.ID_stg = pa.Country_stg 

			LEFT OUTER JOIN DB_T_PROD_STAG.pcx_firedepartment_alfa pfa 

				ON pfa.ID_stg = pl.FireDepartment_alfa_stg 

			INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp 

				ON a.BranchID_stg = pp.ID_stg 

				and (a.ExpirationDate_stg is NULL 

				or a.ExpirationDate_stg > pp.EditEffectiveDate_stg) 

				and pl.Expirationdate_stg is NULL 

			WHERE	a.UpdateTime_stg > (:START_DTTM) 

				AND a.UpdateTime_stg <= (:end_dttm)

				union all

				

	/*EIM-48789*/

				/***** FARM Dwelling  Location ********************************/ 

			SELECT * from (

select	distinct pa.addressline1_stg as addressline1, pa.addressline2_stg as addressline2,

		pa.addressline3_stg as addressline3, pa.postalcode_stg as postalcode,

		cast(pdh.fixedid_stg as varchar(100)) as fixedid, ''PRTY_ASSET_SBTYPE37'' as assettype ,

		''PRTY_ASSET_CLASFCN15'' as ClassificationCD, ''DWELLING'' as Mfg_Home_PrkCD,

		pfa.PublicID_stg as NK_Fire_Dept_ID , cast(null as varchar(3)) FIRE_DEPT_OUT_OF_CNTY_IND,

		pl.cityinternal_stg as city_name, pl.countyinternal_stg as County_Name,

		upper(ps.TYPECODE_stg) as StateTypecode, upper(pc.TYPECODE_stg) as Country,

		''PHYSICAL'' as Locator_RoleCD,pp.EditEffectiveDate_stg as strtdt,

		''SRC_SYS4'' as src_cd, case when (pdh.UpdateTime_stg >phh.UpdateTime_stg ) then pdh.UpdateTime_stg  else phh.UpdateTime_stg  end as trans_strtdt 

FROM	DB_T_PROD_STAG.pcx_fopdwelling pdh /* -EIM-46218  */
INNER JOIN DB_T_PROD_STAG.pcx_foplocation phh 

	ON pdh.location_stg = phh.Fixedid_stg 

INNER JOIN DB_T_PROD_STAG.pc_policylocation pl 

	ON phh.policylocationid_stg = pl.id_stg 

inner join DB_T_PROD_STAG.pc_address pa 

	on pl.AccountLocation_stg = pa.ID_stg join DB_T_PROD_STAG.pctl_state ps 

	on ps.id_stg=pa.State_stg join DB_T_PROD_STAG.pctl_country pc 

	on pc.id_stg=pa.Country_stg 

left outer Join DB_T_PROD_STAG.pcx_firedepartment_alfa pfa 

	on pfa.ID_stg=phh.FireDepartment_stg 

INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp 

	on pdh.branchid_stg=pp.id_stg 

	and phh.BranchID_stg = pp.ID_stg 

	and (pdh.ExpirationDate_stg is NULL 

	or pdh.ExpirationDate_stg > pp.editeffectivedate_stg) 

	and (phh.ExpirationDate_stg is NULL 

	or phh.ExpirationDate_stg > pp.editeffectivedate_stg) 

WHERE	((pdh.UpdateTime_stg>(:START_DTTM) 

	AND pdh.UpdateTime_stg <= (:end_dttm) ) or (phh.UpdateTime_stg>(:START_DTTM) 

	AND phh.UpdateTime_stg <= (:end_dttm)))

	qualify row_number() over(partition by fixedid, pa.addressline1_stg , pa.addressline2_stg,

		pa.addressline3_stg , pa.postalcode_stg,NK_Fire_Dept_ID,city_name,County_Name,StateTypecode,Country 

		order by coalesce(pdh.ExpirationDate_stg, cast(''9999-12-31 23:59:59.999999'' as timestamp(6))) desc,

		 coalesce(phh.ExpirationDate_stg, cast(''9999-12-31 23:59:59.999999'' as timestamp(6))) desc, trans_strtdt desc,pdh.createtime_stg desc)=1

	union /**** FARM Outbuilding Location *******************************/ 

select	distinct pa.addressline1_stg as addressline1,

		pa.addressline2_stg as addressline2,

		pa.addressline3_stg as addressline3,

		pa.postalcode_stg as postalcode,

		cast(pdh.fixedid_stg as varchar(100)) as fixedid,

		''PRTY_ASSET_SBTYPE36'' as assettype ,

		''PRTY_ASSET_CLASFCN13'' as ClassificationCD,

		''DWELLING'' as Mfg_Home_PrkCD,

		pfa.PublicID_stg as NK_Fire_Dept_ID ,

		cast(null as varchar(3)) FIRE_DEPT_OUT_OF_CNTY_IND,

		pl.cityinternal_stg as city_name,

		pl.countyinternal_stg as County_Name,

		upper(ps.TYPECODE_stg) as StateTypecode,

		upper(pc.TYPECODE_stg) as Country,

		''PHYSICAL'' as Locator_RoleCD,

		pp.EditEffectiveDate_stg as strtdt,

		''SRC_SYS4'' as src_cd, case when (pdh.UpdateTime_stg >phh.UpdateTime_stg ) then pdh.UpdateTime_stg  else phh.UpdateTime_stg  end as trans_strtdt 

FROM	DB_T_PROD_STAG.pcx_fopoutbuilding pdh /* -EIM-46218 */
INNER JOIN DB_T_PROD_STAG.pcx_foplocation phh 

	ON pdh.location_stg = phh.Fixedid_stg 

INNER JOIN DB_T_PROD_STAG.pc_policylocation pl 

	ON phh.policylocationid_stg = pl.id_stg 

inner join DB_T_PROD_STAG.pc_address pa 

	on pl.AccountLocation_stg = pa.ID_stg join DB_T_PROD_STAG.pctl_state ps 

	on ps.id_stg=pa.State_stg join DB_T_PROD_STAG.pctl_country pc 

	on pc.id_stg=pa.Country_stg 

left outer Join DB_T_PROD_STAG.pcx_firedepartment_alfa pfa 

	on pfa.ID_stg=phh.FireDepartment_stg 

INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp 

	on pdh.branchid_stg=pp.id_stg 

	and phh.BranchID_stg = pp.ID_stg 

	and (pdh.ExpirationDate_stg is NULL 

	or pdh.ExpirationDate_stg > pp.editeffectivedate_stg) 

	and (phh.ExpirationDate_stg is NULL 

	or phh.ExpirationDate_stg > pp.editeffectivedate_stg) 

WHERE	((pdh.UpdateTime_stg>(:START_DTTM) 

	AND pdh.UpdateTime_stg <= (:end_dttm) ) or (phh.UpdateTime_stg>(:START_DTTM) 

	AND phh.UpdateTime_stg <= (:end_dttm)))

	qualify row_number() over(partition by pa.addressline1_stg , pa.addressline2_stg,

		pa.addressline3_stg , pa.postalcode_stg,NK_Fire_Dept_ID,city_name,County_Name,StateTypecode,Country,

		cast(pdh.fixedid_stg as varchar(100)) 

		order by coalesce(pdh.ExpirationDate_stg, cast(''9999-12-31 23:59:59.999999'' as timestamp(6))) desc,

		 coalesce(phh.ExpirationDate_stg, cast(''9999-12-31 23:59:59.999999'' as timestamp(6))) desc,

		trans_strtdt desc,pdh.createtime_stg desc)=1

		union

		

select	distinct pa.addressline1_stg as addressline1, pa.addressline2_stg as addressline2,

		pa.addressline3_stg as addressline3, pa.postalcode_stg as postalcode,

		cast(pdh.fixedid_stg as varchar(100)) as fixedid, ''PRTY_ASSET_SBTYPE37'' as assettype ,

		''PRTY_ASSET_CLASFCN15'' as ClassificationCD, ''DWELLING'' as Mfg_Home_PrkCD,

		pfa.PublicID_stg as NK_Fire_Dept_ID , cast(null as varchar(3)) FIRE_DEPT_OUT_OF_CNTY_IND,

		pl.cityinternal_stg as city_name, pl.countyinternal_stg as County_Name,

		upper(ps.TYPECODE_stg) as StateTypecode, upper(pc.TYPECODE_stg) as Country,

		''RISK'' as Locator_RoleCD,pp.EditEffectiveDate_stg as strtdt,

		''SRC_SYS4'' as src_cd, case when (pdh.UpdateTime_stg >phh.UpdateTime_stg ) then pdh.UpdateTime_stg  else phh.UpdateTime_stg  end as trans_strtdt 

FROM	DB_T_PROD_STAG.pcx_fopdwelling pdh /* -EIM-46218  */
INNER JOIN DB_T_PROD_STAG.pcx_foplocation phh 

	ON pdh.location_stg = phh.Fixedid_stg 

INNER JOIN DB_T_PROD_STAG.pc_policylocation pl 

	ON phh.policylocationid_stg = pl.id_stg 

inner join DB_T_PROD_STAG.pc_address pa 

	on pl.AccountLocation_stg = pa.ID_stg join DB_T_PROD_STAG.pctl_state ps 

	on ps.id_stg=pa.State_stg join DB_T_PROD_STAG.pctl_country pc 

	on pc.id_stg=pa.Country_stg 

left outer Join DB_T_PROD_STAG.pcx_firedepartment_alfa pfa 

	on pfa.ID_stg=phh.FireDepartment_stg 

INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp 

	on pdh.branchid_stg=pp.id_stg 

	and phh.BranchID_stg = pp.ID_stg 

	and (pdh.ExpirationDate_stg is NULL 

	or pdh.ExpirationDate_stg > pp.editeffectivedate_stg) 

	and (phh.ExpirationDate_stg is NULL 

	or phh.ExpirationDate_stg > pp.editeffectivedate_stg) 

WHERE	((pdh.UpdateTime_stg>(:START_DTTM) 

	AND pdh.UpdateTime_stg <= (:end_dttm) ) or (phh.UpdateTime_stg>(:START_DTTM) 

	AND phh.UpdateTime_stg <= (:end_dttm)))

	qualify row_number() over(partition by fixedid ,pa.addressline1_stg , pa.addressline2_stg,

		pa.addressline3_stg , pa.postalcode_stg,NK_Fire_Dept_ID,city_name,County_Name,StateTypecode,Country 

		order by coalesce(pdh.ExpirationDate_stg, cast(''9999-12-31 23:59:59.999999'' as timestamp(6))) desc,

		 coalesce(phh.ExpirationDate_stg, cast(''9999-12-31 23:59:59.999999'' as timestamp(6))) desc, trans_strtdt desc,pdh.createtime_stg desc)=1

	union /**** FARM Outbuilding Location *******************************/ 

select	distinct pa.addressline1_stg as addressline1,

		pa.addressline2_stg as addressline2,

		pa.addressline3_stg as addressline3,

		pa.postalcode_stg as postalcode,

		cast(pdh.fixedid_stg as varchar(100)) as fixedid,

		''PRTY_ASSET_SBTYPE36'' as assettype ,

		''PRTY_ASSET_CLASFCN13'' as ClassificationCD,

		''DWELLING'' as Mfg_Home_PrkCD,

		pfa.PublicID_stg as NK_Fire_Dept_ID ,

		cast(null as varchar(3)) FIRE_DEPT_OUT_OF_CNTY_IND,

		pl.cityinternal_stg as city_name,

		pl.countyinternal_stg as County_Name,

		upper(ps.TYPECODE_stg) as StateTypecode,

		upper(pc.TYPECODE_stg) as Country,

		''RISK'' as Locator_RoleCD,

		pp.EditEffectiveDate_stg as strtdt,

		''SRC_SYS4'' as src_cd, case when (pdh.UpdateTime_stg >phh.UpdateTime_stg ) then pdh.UpdateTime_stg  else phh.UpdateTime_stg  end as trans_strtdt 

FROM	DB_T_PROD_STAG.pcx_fopoutbuilding pdh /* -EIM-46218 */
INNER JOIN DB_T_PROD_STAG.pcx_foplocation phh 

	ON pdh.location_stg = phh.Fixedid_stg 

INNER JOIN DB_T_PROD_STAG.pc_policylocation pl 

	ON phh.policylocationid_stg = pl.id_stg 

inner join DB_T_PROD_STAG.pc_address pa 

	on pl.AccountLocation_stg = pa.ID_stg join DB_T_PROD_STAG.pctl_state ps 

	on ps.id_stg=pa.State_stg join DB_T_PROD_STAG.pctl_country pc 

	on pc.id_stg=pa.Country_stg 

left outer Join DB_T_PROD_STAG.pcx_firedepartment_alfa pfa 

	on pfa.ID_stg=phh.FireDepartment_stg 

INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp 

	on pdh.branchid_stg=pp.id_stg 

	and phh.BranchID_stg = pp.ID_stg 

	and (pdh.ExpirationDate_stg is NULL 

	or pdh.ExpirationDate_stg > pp.editeffectivedate_stg) 

	and (phh.ExpirationDate_stg is NULL 

	or phh.ExpirationDate_stg > pp.editeffectivedate_stg) 

WHERE	((pdh.UpdateTime_stg>(:START_DTTM) 

	AND pdh.UpdateTime_stg <= (:end_dttm) ) or (phh.UpdateTime_stg>(:START_DTTM) 

	AND phh.UpdateTime_stg <= (:end_dttm)))

	qualify row_number() over(partition by pa.addressline1_stg , pa.addressline2_stg,

		pa.addressline3_stg , pa.postalcode_stg,NK_Fire_Dept_ID,city_name,County_Name,StateTypecode,Country,

		cast(pdh.fixedid_stg as varchar(100)) 

		order by coalesce(pdh.ExpirationDate_stg, cast(''9999-12-31 23:59:59.999999'' as timestamp(6))) desc,

		 coalesce(phh.ExpirationDate_stg, cast(''9999-12-31 23:59:59.999999'' as timestamp(6))) desc,

		trans_strtdt desc,pdh.createtime_stg desc)=1

		)a

/*EIM-48789*/) as a ) as A 

	qualify	row_number() over( 

	partition by AssetKey,AssetType,ClassificationCD,Locator_RoleCD,

			Mfg_Home_PrkCD,NK_Fire_Dept_ID,City_Name,County_Name,StateTypecode,

			Country,addressline1,addressline2,addressline3,SourceCD 

	order by trans_strtdt desc) =1 )SRC /*************************** SQ Query ends here****************************/ /***************************************** ASSET_TYPE *****************************************/ 

	LEFT JOIN ( 

		SELECT	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as PRTY_ASSET_SBTYPE_CD ,

				TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

		FROM	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT 

		WHERE	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''PRTY_ASSET_SBTYPE'' 

			AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31'' )ASSET_SBTYPE 

		ON ASSET_SBTYPE.SRC_IDNTFTN_VAL=SRC.AssetType /***************************************** ASSET_CLASSIFICATION ******************************************/ 

	LEFT JOIN ( 

		SELECT	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as ASSET_CLASFCN_CD ,

				TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

		FROM	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT 

		WHERE	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''PRTY_ASSET_CLASFCN'' 

			AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS in (''DS'', ''GW'') 

			AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31'' )ASSET_CLASFN 

		ON ASSET_CLASFN.SRC_IDNTFTN_VAL=SRC.ClassificationCD /****************************************** DB_T_PROD_CORE.CTRY ******************************************/ 

	LEFT JOIN ( 

		SELECT	DISTINCT ctry_ID, GEOGRCL_AREA_SHRT_NAME 

		FROM	DB_T_PROD_CORE.CTRY CTRY 

		qualify	row_number() over( 

		partition by GEOGRCL_AREA_SHRT_NAME 

		order by edw_end_dttm desc,edw_strt_dttm desc)=1 ) CTRY 

		ON CTRY.GEOGRCL_AREA_SHRT_NAME=SRC.Country /****************************************** DB_T_PROD_CORE.POSTL_CD ******************************************/ 

	LEFT JOIN ( 

		SELECT	DISTINCT POSTL_CD_ID, CTRY_ID, POSTL_CD_NUM 

		FROM	DB_T_PROD_CORE.POSTL_CD

		WHERE	EDW_END_DTTM=''9999-12-31 23:59:59.999999'' 

		qualify	row_number() over( 

		partition by CTRY_ID,POSTL_CD_NUM 

		order by edw_end_dttm desc,edw_strt_dttm desc)=1 ) POSTL_CD 

		ON POSTL_CD.CTRY_ID=CTRY.ctry_ID 

		AND POSTL_CD.POSTL_CD_NUM=SRC.typeCode /****************************************** DB_T_PROD_CORE.TERR ******************************************/ 

	LEFT join ( 

		SELECT	DISTINCT TERR_ID, CTRY_ID, GEOGRCL_AREA_SHRT_NAME 

		FROM	DB_T_PROD_CORE.TERR TERR 

		WHERE	TERR.EDW_END_DTTM=''9999-12-31 23:59:59.999999'' 

		qualify	row_number() over( 

		partition by CTRY_ID,GEOGRCL_AREA_SHRT_NAME 

		order by edw_end_dttm desc,edw_strt_dttm desc)=1 ) TERR 

		ON TERR.CTRY_ID=CTRY.CTRY_ID 

		AND TERR.GEOGRCL_AREA_SHRT_NAME= SRC.StateTypecode /****************************************** DB_T_PROD_CORE.CITY ******************************************/ 

	LEFT join ( 

		SELECT	DISTINCT CITY_ID, TERR_ID, GEOGRCL_AREA_SHRT_NAME 

		FROM	DB_T_PROD_CORE.CITY CITY 

		WHERE	EDW_END_DTTM=''9999-12-31 23:59:59.999999'' 

		qualify	row_number() over( 

		partition by TERR_ID,GEOGRCL_AREA_SHRT_NAME 

		order by edw_end_dttm desc,edw_strt_dttm desc)=1 ) CITY 

		ON CITY.TERR_ID=TERR.TERR_ID 

		AND CITY.GEOGRCL_AREA_SHRT_NAME=SRC.City_Name /****************************************** DB_T_PROD_CORE.CNTY ******************************************/ 

	LEFT join ( 

		SELECT	CNTY_ID, TERR_ID, GEOGRCL_AREA_SHRT_NAME 

		FROM	DB_T_PROD_CORE.CNTY CNTY 

		WHERE	EDW_END_DTTM=''9999-12-31 23:59:59.999999'' 

		qualify	row_number() over( 

		partition by TERR_ID,GEOGRCL_AREA_SHRT_NAME 

		order by edw_end_dttm desc,edw_strt_dttm desc)=1 ) CNTY 

		ON CNTY.TERR_ID=TERR.TERR_ID 

		AND CNTY.GEOGRCL_AREA_SHRT_NAME=SRC.County_Name /****************************************** DB_T_PROD_CORE.STREET_ADDR ******************************************/ 

	LEFT join ( 

		SELECT	STREET_ADDR.STREET_ADDR_ID as STREET_ADDR_ID, STREET_ADDR.DWLNG_TYPE_CD as DWLNG_TYPE_CD,

				STREET_ADDR.CARIER_RTE_TXT as CARIER_RTE_TXT, STREET_ADDR.SPTL_PNT as SPTL_PNT,

				STREET_ADDR.LOCTR_SBTYPE_CD as LOCTR_SBTYPE_CD, STREET_ADDR.ADDR_SBTYPE_CD as ADDR_SBTYPE_CD,

				STREET_ADDR.GEOCODE_STS_TYPE_CD as GEOCODE_STS_TYPE_CD, STREET_ADDR.ADDR_STDZN_TYPE_CD as ADDR_STDZN_TYPE_CD,

				STREET_ADDR.PRCS_ID as PRCS_ID, STREET_ADDR.EDW_STRT_DTTM as EDW_STRT_DTTM,

				STREET_ADDR.EDW_END_DTTM as EDW_END_DTTM, STREET_ADDR.ADDR_LN_1_TXT as ADDR_LN_1_TXT,

				STREET_ADDR.ADDR_LN_2_TXT as ADDR_LN_2_TXT, STREET_ADDR.ADDR_LN_3_TXT as ADDR_LN_3_TXT,

				STREET_ADDR.CITY_ID as CITY_ID, STREET_ADDR.TERR_ID as TERR_ID,

				STREET_ADDR.POSTL_CD_ID as POSTL_CD_ID, STREET_ADDR.CTRY_ID as CTRY_ID,

				STREET_ADDR.CNTY_ID as CNTY_ID 

		FROM	DB_T_PROD_CORE.STREET_ADDR STREET_ADDR 

		WHERE	STREET_ADDR.EDW_END_DTTM=''9999-12-31 23:59:59.999999'' 

			AND STREET_ADDR.ADDR_LN_3_TXT IS NULL 

		qualify	row_number() over( 

		partition by ADDR_LN_1_TXT,ADDR_LN_2_TXT,ADDR_LN_3_TXT,CITY_ID,

				TERR_ID,POSTL_CD_ID,CTRY_ID,CNTY_ID,EDW_END_DTTM 

		order by edw_end_dttm desc,edw_strt_dttm desc)=1 ) lkp_street_addr 

		ON lkp_street_addr.ADDR_LN_1_TXT = SRC.AddressLine1 

		AND coalesce(lkp_street_addr.ADDR_LN_2_TXT, ''~'') = coalesce(SRC.AddressLine2,

			''~'') 

		AND lkp_street_addr.CITY_ID = CITY.CITY_ID 

		AND lkp_street_addr.TERR_ID = TERR.TERR_ID 

		AND lkp_street_addr.POSTL_CD_ID = POSTL_CD.POSTL_CD_ID 

		AND lkp_street_addr.CTRY_ID = CTRY.CTRY_ID 

		AND coalesce(lkp_street_addr.CNTY_ID, ''~'') = coalesce(CNTY.CNTY_ID,

			''~'') /****************************************** SOURCE_CD ******************************************/ 

	LEFT JOIN ( 

		SELECT	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL ,

				TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

		FROM	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT 

		WHERE	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''SRC_SYS'' 

			AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived'' 

			AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 

			AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31'' )SRC_CD 

		ON SRC_CD.SRC_IDNTFTN_VAL=SRC.SourceCD /****************************************** DB_T_PROD_CORE.FIRE_DEPT ******************************************/ 

	LEFT JOIN ( 

		SELECT	DISTINCT FIRE_DEPT_ID,HOST_FIRE_DEPT_NUM 

		FROM	DB_T_PROD_CORE.FIRE_DEPT ) FIRE_DEPT 

		ON FIRE_DEPT.HOST_FIRE_DEPT_NUM=SRC.NK_Fire_Dept_ID /****************************************** DB_T_PROD_CORE.PRTY_ASSET ******************************************/ 

	LEFT JOIN ( 

		SELECT	PRTY_ASSET.PRTY_ASSET_ID as PRTY_ASSET_ID, PRTY_ASSET.ASSET_INSRNC_HIST_TYPE_CD as ASSET_INSRNC_HIST_TYPE_CD,

				PRTY_ASSET.ASSET_DESC as ASSET_DESC, PRTY_ASSET.PRTY_ASSET_NAME as PRTY_ASSET_NAME,

				PRTY_ASSET.PRTY_ASSET_STRT_DTTM as PRTY_ASSET_STRT_DTTM, PRTY_ASSET.PRTY_ASSET_END_DTTM as PRTY_ASSET_END_DTTM,

				PRTY_ASSET.EDW_STRT_DTTM as EDW_STRT_DTTM, PRTY_ASSET.EDW_END_DTTM as EDW_END_DTTM,

				PRTY_ASSET.SRC_SYS_CD as SRC_SYS_CD, PRTY_ASSET.ASSET_HOST_ID_VAL as ASSET_HOST_ID_VAL,

				PRTY_ASSET.PRTY_ASSET_SBTYPE_CD as PRTY_ASSET_SBTYPE_CD, PRTY_ASSET.PRTY_ASSET_CLASFCN_CD as PRTY_ASSET_CLASFCN_CD 

		FROM	DB_T_PROD_CORE.PRTY_ASSET 

		QUALIFY	ROW_NUMBER() OVER( 

		PARTITION BY ASSET_HOST_ID_VAL,PRTY_ASSET_SBTYPE_CD,PRTY_ASSET_CLASFCN_CD 

		ORDER BY EDW_END_DTTM DESC) = 1 ) PRTY_ASSET 

		ON PRTY_ASSET.ASSET_HOST_ID_VAL=SRC.AssetKey 

		AND PRTY_ASSET.PRTY_ASSET_SBTYPE_CD=ASSET_SBTYPE.PRTY_ASSET_SBTYPE_CD 

		AND PRTY_ASSET.PRTY_ASSET_CLASFCN_CD=ASSET_CLASFN.ASSET_CLASFCN_CD ) ,

		normalizer as ( 

	select	PRTY_ASSET_ID as PRTY_ASSET_ID, LOCATOR_ROLECD, Mfg_Home_PrkCD,

			strtdt, LOC_ID_in1 as LOC_ID, 1 as GCID_LOC_ID,FIRE_DEPT_ID,

			FIRE_DEPT_OUT_OF_CNTY_IND, trans_strtdt 

	from LOC 

	UNION ALL 

	select	PRTY_ASSET_ID as PRTY_ASSET_ID, LOCATOR_ROLECD, Mfg_Home_PrkCD,

			strtdt, LOC_ID_in2 as LOC_ID, 2 as GCID_LOC_ID,FIRE_DEPT_ID,

			FIRE_DEPT_OUT_OF_CNTY_IND, trans_strtdt 

	from	LOC 

	UNION ALL 

	select	PRTY_ASSET_ID as PRTY_ASSET_ID, LOCATOR_ROLECD, Mfg_Home_PrkCD,

			strtdt, LOC_ID_in3 as LOC_ID, 3 as GCID_LOC_ID,FIRE_DEPT_ID,

			FIRE_DEPT_OUT_OF_CNTY_IND, trans_strtdt 

	from	LOC 

	UNION ALL 

	select	PRTY_ASSET_ID as PRTY_ASSET_ID, LOCATOR_ROLECD, Mfg_Home_PrkCD,

			strtdt, LOC_ID_in4 as LOC_ID, 4 as GCID_LOC_ID,FIRE_DEPT_ID,

			FIRE_DEPT_OUT_OF_CNTY_IND, trans_strtdt 

	from	LOC 

	UNION ALL 

	select	PRTY_ASSET_ID as PRTY_ASSET_ID, LOCATOR_ROLECD, Mfg_Home_PrkCD,

			strtdt, LOC_ID_in5 as LOC_ID, 5 as GCID_LOC_ID,FIRE_DEPT_ID,

			FIRE_DEPT_OUT_OF_CNTY_IND, trans_strtdt 

	from	LOC ) /******************************** LOADING QUERY *****************************************************************/ 

select	distinct SRC_PRTY_ASSET_ID,SRC_LOC_ID,SRC_LOCATOR_ROLECD,

		SRC_CODE,SRC_STRTDT,cast(''9999-12-31 23:59:59.999999'' as timestamp) as SRC_PRTY_ASSET_LOCTR_END_DTTM,

		SRC_FIRE_DEPT_ID,SRC_FIRE_DEPT_OUT_OF_CNTY_IND,SRC_LOC_ROLE_CD,

		SRC_TRANS_STRT_DT,SRC_PRTY_ASSET_LOCTR_ROLE,RANKINDEX, EDW_STRT_DTTM,

		EDW_END_DTTM,TGT_LOC_ID, TGT_PRTY_ASSET_LOCTR_STRT_DTTM, TGT_PRTY_ASSET_LOCTR_END_DTTM,

		TGT_FIRE_DEPT_ID, TGT_PRTY_ASSET_ID,TGT_PRTY_ASSET_LOCTR_ROLE_CD,

/* TARGET DATA*/ CAST(TRIM(COALESCE(cast(TGT_PRTY_ASSET_LOCTR_STRT_DTTM as varchar(100)), 
		''9999''))|| TRIM(COALESCE(cast(TGT_PRTY_ASSET_LOCTR_END_DTTM as varchar(100)),

		''9999''))|| TRIM(COALESCE(cast(TGT_FIRE_DEPT_ID as varchar(100)),

		''9999''))|| TRIM(COALESCE(cast(TGT_LOC_ID as varchar(100)),''9999''))||TRIM(COALESCE(cast(TGT_FIRE_DEPT_OUT_OF_CNTY_IND as varchar(100)),

''9999'')) AS VARCHAR(1000)) as TARGETDATA /* SOURCE DATA*/ ,CAST(TRIM(COALESCE(cast(SRC_STRTDT as varchar(100)), 
		''9999''))|| TRIM(COALESCE(cast(SRC_PRTY_ASSET_LOCTR_END_DTTM as varchar(100)),

		''9999''))|| TRIM(COALESCE(cast(SRC_FIRE_DEPT_ID as varchar(100)),

		''9999''))|| TRIM(COALESCE(cast(SRC_LOC_ID as varchar(100)),''9999''))||TRIM(COALESCE(cast(SRC_FIRE_DEPT_OUT_OF_CNTY_IND as varchar(100)),

''9999''))AS VARCHAR(1000)) as SOURCEDATA /*FLAG*/ ,  
		CASE 

			WHEN TARGETDATA IS NULL 

	OR (TGT_LOC_ID IS NULL 

	AND TGT_PRTY_ASSET_ID IS NULL) THEN ''I'' 

			WHEN TARGETDATA IS NOT NULL 

	AND (TARGETDATA <> SOURCEDATA) THEN ''U'' 

			WHEN TARGETDATA IS NOT NULL 

	AND (TARGETDATA = SOURCEDATA) THEN ''R'' 

		END AS calc_ins_upd,TGT_FIRE_DEPT_OUT_OF_CNTY_IND 

FROM	( 

	select	distinct MQ2.PRTY_ASSET_ID AS SRC_PRTY_ASSET_ID,MQ2.LOC_ID AS SRC_LOC_ID,

			LOCATOR_ROLECD AS SRC_LOCATOR_ROLECD, CODE AS SRC_CODE,strtdt AS SRC_STRTDT,

			MQ2.FIRE_DEPT_ID AS SRC_FIRE_DEPT_ID,MQ2.FIRE_DEPT_OUT_OF_CNTY_IND AS SRC_FIRE_DEPT_OUT_OF_CNTY_IND,

			GCID_LOC_ID,Loc_Role_CD AS SRC_LOC_ROLE_CD, TRANS_STRT_DT AS SRC_TRANS_STRT_DT,

			PRTY_ASSET_LOCTR_ROLE AS SRC_PRTY_ASSET_LOCTR_ROLE, ROW_NUMBER() OVER( 

PARTITION BY MQ2.PRTY_ASSET_ID, Loc_Role_CD /* EIM-48807 */
	ORDER BY TRANS_STRT_DT) as RANKINDEX, 
	--CAST(current_timestamp + (RANKINDEX - 1) * INTERVAL ''2 SECOND'' AS TIMESTAMP) AS EDW_STRT_DTTM,
	DATEADD(
  ''SECOND'',
  (RANKINDEX - 1) * 2,
  CURRENT_TIMESTAMP()
) AS EDW_STRT_DTTM,

/* Added as part of EIM-42657*/ cast(''9999-12-31 23:59:59.999999'' as timestamp) as EDW_END_DTTM, 
			TGT_PAL.LOC_ID AS TGT_LOC_ID, TGT_PAL.PRTY_ASSET_LOCTR_STRT_DTTM as TGT_PRTY_ASSET_LOCTR_STRT_DTTM,

			TGT_PAL.PRTY_ASSET_LOCTR_END_DTTM as TGT_PRTY_ASSET_LOCTR_END_DTTM,

			TGT_PAL.FIRE_DEPT_ID as TGT_FIRE_DEPT_ID, TGT_PAL.FIRE_DEPT_OUT_OF_CNTY_IND AS TGT_FIRE_DEPT_OUT_OF_CNTY_IND ,

			TGT_PAL.PRTY_ASSET_ID as TGT_PRTY_ASSET_ID, TGT_PAL.PRTY_ASSET_LOCTR_ROLE_CD as TGT_PRTY_ASSET_LOCTR_ROLE_CD 

	from	( 

		select	distinct PRTY_ASSET_ID, LOC_ID,LOCATOR_ROLECD,CODE,strtdt,

				FIRE_DEPT_ID,FIRE_DEPT_OUT_OF_CNTY_IND,GCID_LOC_ID,Loc_Role_CD,

				TRANS_STRT_DT,LOCTR_ROLE.TGT_IDNTFTN_VAL AS PRTY_ASSET_LOCTR_ROLE 

		from	( 

			select	PRTY_ASSET_ID, LOC_ID,LOCATOR_ROLECD,max(CODE) as CODE,

					max(strtdt) as strtdt,max(FIRE_DEPT_ID)as FIRE_DEPT_ID, max(FIRE_DEPT_OUT_OF_CNTY_IND) AS FIRE_DEPT_OUT_OF_CNTY_IND,

					max(GCID_LOC_ID) as GCID_LOC_ID,max(Loc_Role_CD) as Loc_Role_CD,

					max(TRANS_STRT_DT) as TRANS_STRT_DT 

			FROM	( 

				select	distinct PRTY_ASSET_ID, LOC_ID,LOCATOR_ROLECD,CODE, 

						case 

							when (strtdt is null) then cast(''1900-01-01 00:00:00.000000'' as timestamp(6)) 

							else cast(strtdt as timestamp(6)) 

end as strtdt, /* strtdt,*/ FIRE_DEPT_ID,FIRE_DEPT_OUT_OF_CNTY_IND, 
						GCID_LOC_ID, 

						CASE 

							WHEN GCID_LOC_ID=1 

					and LOCATOR_ROLECD=''RISK'' 

					and (CODE=''VEHICLE'' 

					OR CODE=''DWELLING'' 

					OR CODE=''BUILDING'') THEN ''PRTY_ASSET_LOCTR_ROLE5'' 

							WHEN GCID_LOC_ID=2 

					and LOCATOR_ROLECD=''RISK'' 

					and (CODE=''VEHICLE'' 

					OR CODE=''DWELLING'' 

					OR CODE=''BUILDING'') THEN ''PRTY_ASSET_LOCTR_ROLE6'' 

							WHEN GCID_LOC_ID=3 

					and LOCATOR_ROLECD=''RISK'' 

					and (CODE=''VEHICLE'' 

					OR CODE=''DWELLING'' 

					OR CODE=''BUILDING'') THEN ''PRTY_ASSET_LOCTR_ROLE4'' 

							WHEN GCID_LOC_ID=4 

					and LOCATOR_ROLECD=''RISK'' 

					and (CODE=''VEHICLE'' 

					OR CODE=''DWELLING'' 

					OR CODE=''BUILDING'') THEN ''PRTY_ASSET_LOCTR_ROLE8'' 

							WHEN GCID_LOC_ID=5 

					and LOCATOR_ROLECD=''RISK'' 

					and (CODE=''VEHICLE'' 

					OR CODE=''DWELLING'' 

					OR CODE=''BUILDING'') THEN ''PRTY_ASSET_LOCTR_ROLE7'' 

							WHEN GCID_LOC_ID=1 

					and LOCATOR_ROLECD=''GARAGE'' 

					and CODE=''VEHICLE'' THEN ''PRTY_ASSET_LOCTR_ROLE10'' 

							WHEN GCID_LOC_ID=2 

					and LOCATOR_ROLECD=''GARAGE'' 

					and CODE=''VEHICLE'' THEN ''PRTY_ASSET_LOCTR_ROLE11'' 

							WHEN GCID_LOC_ID=3 

					and LOCATOR_ROLECD=''GARAGE'' 

					and CODE=''VEHICLE'' THEN ''PRTY_ASSET_LOCTR_ROLE9'' 

							WHEN GCID_LOC_ID=4 

					and LOCATOR_ROLECD=''GARAGE'' 

					and CODE=''VEHICLE'' THEN ''PRTY_ASSET_LOCTR_ROLE13'' 

							WHEN GCID_LOC_ID=5 

					and LOCATOR_ROLECD=''GARAGE'' 

					and CODE=''VEHICLE'' THEN ''PRTY_ASSET_LOCTR_ROLE12'' 

							WHEN GCID_LOC_ID=1 

					and LOCATOR_ROLECD=''RISK'' 

					and CODE=''PERSONAL'' THEN ''PRTY_ASSET_LOCTR_ROLE5'' 

							WHEN GCID_LOC_ID=2 

					and LOCATOR_ROLECD=''RISK'' 

					and CODE=''PERSONAL'' THEN ''PRTY_ASSET_LOCTR_ROLE6'' 

							WHEN GCID_LOC_ID=3 

					and LOCATOR_ROLECD=''RISK'' 

					and CODE=''PERSONAL'' THEN ''PRTY_ASSET_LOCTR_ROLE4'' 

							WHEN GCID_LOC_ID=4 

					and LOCATOR_ROLECD=''RISK'' 

					and CODE=''PERSONAL'' THEN ''PRTY_ASSET_LOCTR_ROLE8'' 

							WHEN GCID_LOC_ID=5 

					and LOCATOR_ROLECD=''RISK'' 

					and CODE=''PERSONAL'' THEN ''PRTY_ASSET_LOCTR_ROLE7'' 

							WHEN GCID_LOC_ID=1 

					and LOCATOR_ROLECD=''RISK'' 

					and CODE=''CC'' THEN ''PRTY_ASSET_LOCTR_ROLE5'' 

							WHEN GCID_LOC_ID=1 

					and LOCATOR_ROLECD=''GARAGE'' 

					and CODE=''CC'' THEN ''PRTY_ASSET_LOCTR_ROLE10'' 

					        WHEN GCID_LOC_ID=1 

					and LOCATOR_ROLECD=''CLAIM'' 

and CODE=''VEHICLE'' THEN ''PRTY_ASSET_LOCTR_ROLE20''/* EIM-47230--CLMST */
						    WHEN GCID_LOC_ID=1 

					AND LOCATOR_ROLECD=''RISK'' 

					and CODE=''DWELLING'' THEN ''PRTY_ASSET_LOCTR_ROLE5'' 

							WHEN GCID_LOC_ID=1 

					and LOCATOR_ROLECD=''PHYSICAL'' 

					AND (CODE=''DWELLING'' 

					OR CODE=''BUILDING'') THEN ''PRTY_ASSET_LOCTR_ROLE15'' 

							WHEN GCID_LOC_ID=2 

					and LOCATOR_ROLECD=''PHYSICAL'' 

					AND (CODE=''DWELLING'' 

					OR CODE=''BUILDING'') THEN ''PRTY_ASSET_LOCTR_ROLE16'' 

							WHEN GCID_LOC_ID=3 

					and LOCATOR_ROLECD=''PHYSICAL'' 

					AND (CODE=''DWELLING'' 

					OR CODE=''BUILDING'') THEN ''PRTY_ASSET_LOCTR_ROLE14'' 

							WHEN GCID_LOC_ID=4 

					and LOCATOR_ROLECD=''PHYSICAL'' 

					AND (CODE=''DWELLING'' 

					OR CODE=''BUILDING'') THEN ''PRTY_ASSET_LOCTR_ROLE18'' 

							WHEN GCID_LOC_ID=5 

					and LOCATOR_ROLECD=''PHYSICAL'' 

					AND (CODE=''DWELLING'' 

					OR CODE=''BUILDING'') THEN ''PRTY_ASSET_LOCTR_ROLE17'' 

							WHEN GCID_LOC_ID=1 

					and LOCATOR_ROLECD=''RISK'' 

					and CODE=''STATE'' THEN ''PRTY_ASSET_LOCTR_ROLE19'' 

							ELSE ''UNK'' 

						END AS Loc_Role_CD, 

						case 

							when (trans_strtdt is null) then cast(''1900-01-01 00:00:00.000000'' as timestamp(6)) 

							else cast(TRANS_STRTDT as timestamp(6)) 

						end as TRANS_STRT_DT 

				from( /*Main Query Result after Normalizer*/ 

				select	distinct PRTY_ASSET_ID, LOC_ID,LOCATOR_ROLECD, Mfg_Home_PrkCD as CODE ,

						strtdt,FIRE_DEPT_ID,FIRE_DEPT_OUT_OF_CNTY_IND,trans_strtdt, GCID_LOC_ID 

				from	normalizer )MQ )grp 

			group by PRTY_ASSET_ID, LOC_ID,LOCATOR_ROLECD )MQ1 /****************************************** PRTY_ASSET_LOCTR_ROLE ******************************************/ 

		LEFT JOIN ( 

			SELECT	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL ,

					TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

			FROM	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT 

			WHERE	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''PRTY_ASSET_LOCTR_ROLE'' 

				AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived'' 

				AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 

				AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31'' )LOCTR_ROLE 

			ON MQ1.Loc_Role_CD=LOCTR_ROLE.SRC_IDNTFTN_VAL )MQ2 /****************************************** TARGET LOOKUP ******************************************/ 

	LEFT JOIN ( 

		SELECT	PRTY_ASSET_LOCTR.LOC_ID as LOC_ID, PRTY_ASSET_LOCTR.PRTY_ASSET_LOCTR_STRT_DTTM as PRTY_ASSET_LOCTR_STRT_DTTM,

				PRTY_ASSET_LOCTR.PRTY_ASSET_LOCTR_END_DTTM as PRTY_ASSET_LOCTR_END_DTTM,

				PRTY_ASSET_LOCTR.FIRE_DEPT_ID as FIRE_DEPT_ID, PRTY_ASSET_LOCTR.FIRE_DEPT_OUT_OF_CNTY_IND,

				PRTY_ASSET_LOCTR.PRTY_ASSET_ID as PRTY_ASSET_ID, PRTY_ASSET_LOCTR.PRTY_ASSET_LOCTR_ROLE_CD as PRTY_ASSET_LOCTR_ROLE_CD 

		FROM	DB_T_PROD_CORE.PRTY_ASSET_LOCTR 

		QUALIFY	ROW_NUMBER() OVER( 

		PARTITION BY PRTY_ASSET_ID, PRTY_ASSET_LOCTR_ROLE_CD 

		ORDER BY EDW_END_DTTM desc) = 1 )TGT_PAL 

		ON MQ2.PRTY_ASSET_ID=TGT_PAL.PRTY_ASSET_ID 

		AND PRTY_ASSET_LOCTR_ROLE=TGT_PAL.PRTY_ASSET_LOCTR_ROLE_CD 

	where	locator_rolecd<>''UNK'' 

		and MQ2.LOC_ID is not null )OVERALL 

where	(SRC_PRTY_ASSET_ID is not null ) 

	and ((Calc_ins_upd =''I'') 

	or ((Calc_ins_upd =''U'') 

	and (SRC_STRTDT>TGT_PRTY_ASSET_LOCTR_STRT_DTTM)))
) SRC
)
);


-- Component exp_data_transformation, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_data_transformation AS
(
SELECT
SQ_prty_asset_loctr_x.SRC_PRTY_ASSET_ID as SRC_PRTY_ASSET_ID,
SQ_prty_asset_loctr_x.SRC_LOC_ID as SRC_LOC_ID,
SQ_prty_asset_loctr_x.SRC_LOCATOR_ROLE_CD as SRC_LOCATOR_ROLE_CD,
SQ_prty_asset_loctr_x.SRC_CODE as SRC_CODE,
SQ_prty_asset_loctr_x.SRC_START_DT as SRC_START_DT,
SQ_prty_asset_loctr_x.SRC_PRTY_ASSET_LOCTR_END_DTTM as SRC_PRTY_ASSET_LOCTR_END_DTTM,
SQ_prty_asset_loctr_x.SRC_FIRE_DEPT_ID as SRC_FIRE_DEPT_ID,
SQ_prty_asset_loctr_x.SRC_FIRE_DEPT_OUT_OF_CNTY_IND as SRC_FIRE_DEPT_OUT_OF_CNTY_IND,
SQ_prty_asset_loctr_x.SRC_LOC_ROLE_CD as SRC_LOC_ROLE_CD,
SQ_prty_asset_loctr_x.SRC_TRANS_STRT_DTTM as SRC_TRANS_STRT_DTTM,
SQ_prty_asset_loctr_x.SRC_PRTY_ASSET_LOCTR_ROLE as SRC_PRTY_ASSET_LOCTR_ROLE,
SQ_prty_asset_loctr_x.RANKINDEX as RANKINDEX,
SQ_prty_asset_loctr_x.EDW_STRT_DTTM as EDW_STRT_DTTM,
SQ_prty_asset_loctr_x.EDW_END_DTTM as EDW_END_DTTM,
SQ_prty_asset_loctr_x.TGT_LOC_ID as TGT_LOC_ID,
SQ_prty_asset_loctr_x.TGT_PRTY_ASSET_LOCTR_STRT_DTTM as TGT_PRTY_ASSET_LOCTR_STRT_DTTM,
SQ_prty_asset_loctr_x.TGT_PRTY_ASSET_LOCTR_END_DTTM as TGT_PRTY_ASSET_LOCTR_END_DTTM,
SQ_prty_asset_loctr_x.TGT_FIRE_DEPT_ID as TGT_FIRE_DEPT_ID,
SQ_prty_asset_loctr_x.TGT_PRTY_ASSET_ID as TGT_PRTY_ASSET_ID,
SQ_prty_asset_loctr_x.TGT_PRTY_ASSET_LOCTR_ROLE_CD as TGT_PRTY_ASSET_LOCTR_ROLE_CD,
SQ_prty_asset_loctr_x.TARGETDATA as TARGETDATA,
SQ_prty_asset_loctr_x.SOURCEDATA as SOURCEDATA,
SQ_prty_asset_loctr_x.FLAG as FLAG,
SQ_prty_asset_loctr_x.source_record_id
FROM
SQ_prty_asset_loctr_x
);


-- Component updstg_ins, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE updstg_ins AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
exp_data_transformation.SRC_PRTY_ASSET_ID as SRC_PRTY_ASSET_ID1,
exp_data_transformation.SRC_LOC_ID as SRC_LOC_ID1,
exp_data_transformation.SRC_LOCATOR_ROLE_CD as SRC_LOCATOR_ROLE_CD1,
exp_data_transformation.SRC_CODE as SRC_CODE1,
exp_data_transformation.SRC_START_DT as SRC_START_DT1,
exp_data_transformation.SRC_PRTY_ASSET_LOCTR_END_DTTM as SRC_PRTY_ASSET_LOCTR_END_DTTM1,
exp_data_transformation.SRC_FIRE_DEPT_ID as SRC_FIRE_DEPT_ID1,
exp_data_transformation.SRC_FIRE_DEPT_OUT_OF_CNTY_IND as SRC_FIRE_DEPT_OUT_OF_CNTY_IND,
exp_data_transformation.SRC_LOC_ROLE_CD as SRC_LOC_ROLE_CD1,
exp_data_transformation.SRC_TRANS_STRT_DTTM as SRC_TRANS_STRT_DTTM1,
exp_data_transformation.SRC_PRTY_ASSET_LOCTR_ROLE as SRC_PRTY_ASSET_LOCTR_ROLE1,
exp_data_transformation.RANKINDEX as RANKINDEX1,
exp_data_transformation.EDW_STRT_DTTM as EDW_STRT_DTTM1,
exp_data_transformation.EDW_END_DTTM as EDW_END_DTTM1,
exp_data_transformation.TGT_LOC_ID as TGT_LOC_ID1,
exp_data_transformation.TGT_PRTY_ASSET_LOCTR_STRT_DTTM as TGT_PRTY_ASSET_LOCTR_STRT_DTTM1,
exp_data_transformation.TGT_PRTY_ASSET_LOCTR_END_DTTM as TGT_PRTY_ASSET_LOCTR_END_DTTM1,
exp_data_transformation.TGT_FIRE_DEPT_ID as TGT_FIRE_DEPT_ID1,
exp_data_transformation.TGT_PRTY_ASSET_ID as TGT_PRTY_ASSET_ID1,
exp_data_transformation.TGT_PRTY_ASSET_LOCTR_ROLE_CD as TGT_PRTY_ASSET_LOCTR_ROLE_CD1,
exp_data_transformation.TARGETDATA as TARGETDATA1,
exp_data_transformation.SOURCEDATA as SOURCEDATA1,
exp_data_transformation.FLAG as FLAG1,
0 as UPDATE_STRATEGY_ACTION,
source_record_id
FROM
exp_data_transformation
);


-- Component exp_pass_src_tgt, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_src_tgt AS
(
SELECT
updstg_ins.SRC_PRTY_ASSET_ID1 as SRC_PRTY_ASSET_ID1,
updstg_ins.SRC_LOC_ID1 as SRC_LOC_ID1,
updstg_ins.SRC_START_DT1 as SRC_START_DT1,
updstg_ins.SRC_PRTY_ASSET_LOCTR_END_DTTM1 as SRC_PRTY_ASSET_LOCTR_END_DTTM1,
updstg_ins.SRC_FIRE_DEPT_ID1 as SRC_FIRE_DEPT_ID1,
updstg_ins.SRC_FIRE_DEPT_OUT_OF_CNTY_IND as SRC_FIRE_DEPT_OUT_OF_CNTY_IND,
updstg_ins.SRC_TRANS_STRT_DTTM1 as SRC_TRANS_STRT_DTTM1,
updstg_ins.SRC_PRTY_ASSET_LOCTR_ROLE1 as SRC_PRTY_ASSET_LOCTR_ROLE1,
updstg_ins.EDW_STRT_DTTM1 as EDW_STRT_DTTM1,
updstg_ins.EDW_END_DTTM1 as EDW_END_DTTM1,
to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) as SRC_TRANS_END_DTTM,
:PRCS_ID as PRCS_ID,
updstg_ins.source_record_id
FROM
updstg_ins
);


-- Component PRTY_ASSET_LOCTR_ins, Type TARGET 
INSERT INTO DB_T_PROD_CORE.PRTY_ASSET_LOCTR
(
PRTY_ASSET_ID,
PRTY_ASSET_LOCTR_ROLE_CD,
PRTY_ASSET_LOCTR_STRT_DTTM,
LOC_ID,
PRTY_ASSET_LOCTR_END_DTTM,
PRCS_ID,
FIRE_DEPT_ID,
FIRE_DEPT_OUT_OF_CNTY_IND,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM,
TRANS_END_DTTM
)
SELECT
exp_pass_src_tgt.SRC_PRTY_ASSET_ID1 as PRTY_ASSET_ID,
exp_pass_src_tgt.SRC_PRTY_ASSET_LOCTR_ROLE1 as PRTY_ASSET_LOCTR_ROLE_CD,
exp_pass_src_tgt.SRC_START_DT1 as PRTY_ASSET_LOCTR_STRT_DTTM,
exp_pass_src_tgt.SRC_LOC_ID1 as LOC_ID,
exp_pass_src_tgt.SRC_PRTY_ASSET_LOCTR_END_DTTM1 as PRTY_ASSET_LOCTR_END_DTTM,
exp_pass_src_tgt.PRCS_ID as PRCS_ID,
exp_pass_src_tgt.SRC_FIRE_DEPT_ID1 as FIRE_DEPT_ID,
exp_pass_src_tgt.SRC_FIRE_DEPT_OUT_OF_CNTY_IND as FIRE_DEPT_OUT_OF_CNTY_IND,
exp_pass_src_tgt.EDW_STRT_DTTM1 as EDW_STRT_DTTM,
exp_pass_src_tgt.EDW_END_DTTM1 as EDW_END_DTTM,
exp_pass_src_tgt.SRC_TRANS_STRT_DTTM1 as TRANS_STRT_DTTM,
exp_pass_src_tgt.SRC_TRANS_END_DTTM as TRANS_END_DTTM
FROM
exp_pass_src_tgt;


-- PIPELINE END FOR 1

-- PIPELINE START FOR 2

-- Component SQ_prty_asset_loctr_x1, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_prty_asset_loctr_x1 AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as addressline1,
$2 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select distinct

cast(null as varchar(100)) as addressline1

from DB_T_PROD_STAG.cc_vehicle ccv

WHERE 1=2
) SRC
)
);


-- Component PRTY_ASSET_LOCTR_ins1, Type TARGET 
INSERT INTO DB_T_PROD_CORE.PRTY_ASSET_LOCTR
(
PRTY_ASSET_LOCTR_ROLE_CD
)
SELECT
SQ_prty_asset_loctr_x1.addressline1 as PRTY_ASSET_LOCTR_ROLE_CD
FROM
SQ_prty_asset_loctr_x1;


-- PIPELINE END FOR 2
-- Component PRTY_ASSET_LOCTR_ins1, Type Post SQL 
UPDATE  db_t_prod_core.PRTY_ASSET_LOCTR  
SET 

	EDW_END_DTTM=TMPLEAD.EDW_LEAD

	,TRANS_END_DTTM=  TMPLEAD.TRANS_LEAD
FROM  

(

SELECT	DISTINCT PRTY_ASSET_ID,PRTY_ASSET_LOCTR_ROLE_CD,EDW_STRT_DTTM,TRANS_STRT_DTTM,PRTY_ASSET_LOCTR_STRT_DTTM,

MAX(TRANS_STRT_DTTM) OVER (PARTITION BY PRTY_ASSET_ID,PRTY_ASSET_LOCTR_ROLE_CD  ORDER BY TRANS_STRT_DTTM  ASC, EDW_STRT_DTTM ASC ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) - INTERVAL ''1 SECOND'' AS TRANS_LEAD,

MAX(EDW_STRT_DTTM) OVER (PARTITION BY PRTY_ASSET_ID,PRTY_ASSET_LOCTR_ROLE_CD  ORDER BY TRANS_STRT_DTTM  ASC, EDW_STRT_DTTM ASC ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) - INTERVAL ''1 SECOND'' AS EDW_LEAD,

RANK() OVER (PARTITION BY PRTY_ASSET_ID,PRTY_ASSET_LOCTR_ROLE_CD ORDER BY TRANS_STRT_DTTM DESC,EDW_STRT_DTTM DESC) AS RANKIDX 

FROM db_t_prod_core.PRTY_ASSET_LOCTR WHERE TRANS_END_DTTM=''9999-12-31 23:59:59.999999''

) TMPLEAD


WHERE  PRTY_ASSET_LOCTR.PRTY_ASSET_ID=TMPLEAD.PRTY_ASSET_ID

AND PRTY_ASSET_LOCTR.PRTY_ASSET_LOCTR_ROLE_CD=TMPLEAD.PRTY_ASSET_LOCTR_ROLE_CD

AND PRTY_ASSET_LOCTR.EDW_STRT_DTTM=TMPLEAD.EDW_STRT_DTTM

AND PRTY_ASSET_LOCTR.TRANS_STRT_DTTM=TMPLEAD.TRANS_STRT_DTTM

AND TMPLEAD.RANKIDX <> 1

AND TMPLEAD.TRANS_LEAD IS NOT NULL;

-- Script updated as part of EIM-42657;


END; ';