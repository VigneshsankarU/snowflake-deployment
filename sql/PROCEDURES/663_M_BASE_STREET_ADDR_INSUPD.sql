-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_STREET_ADDR_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
DECLARE
      run_id varchar;
	start_dttm timestamp;
	end_dttm timestamp;
    prcs_id int;


BEGIN 
 run_id :=   (SELECT run_id   FROM control_run_id where upper(worklet_name) =upper(:worklet_name) order by insert_ts desc limit 1);   
 END_DTTM:=   (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''END_DTTM'' order by insert_ts desc limit 1);
 START_DTTM:= (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''START_DTTM'' order by insert_ts desc limit 1);
 PRCS_ID:=     (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PRCS_ID'' order by insert_ts desc limit 1);




-- Component LKP_CITY, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_CITY AS
(
SELECT CITY.CITY_ID as CITY_ID, CITY.EDW_STRT_DTTM as EDW_STRT_DTTM, CITY.EDW_END_DTTM as EDW_END_DTTM, CITY.TERR_ID as TERR_ID, CITY.GEOGRCL_AREA_SHRT_NAME as GEOGRCL_AREA_SHRT_NAME 
FROM db_t_prod_core.CITY 
QUALIFY ROW_NUMBER() OVER(PARTITION BY TERR_ID, GEOGRCL_AREA_SHRT_NAME  
ORDER BY EDW_END_DTTM desc) = 1
/* WHERE CITY.EDW_END_DTTM=TO_DATE(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'') */
);


-- Component LKP_COUNTY, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_COUNTY AS
(
SELECT CNTY.CNTY_ID as CNTY_ID, CNTY.TERR_ID as TERR_ID, CNTY.GEOGRCL_AREA_SBTYPE_CD as GEOGRCL_AREA_SBTYPE_CD

, ltrim(rtrim(GEOGRCL_AREA_SHRT_NAME)) as GEOGRCL_AREA_SHRT_NAME,LOCTR_SBTYPE_CD as LOCTR_SBTYPE_CD

,GEOGRCL_AREA_STRT_DTTM as GEOGRCL_AREA_STRT_DTTM,EDW_END_DTTM as EDW_END_DTTM 

FROM db_t_prod_core.CNTY



QUALIFY ROW_NUMBER() OVER(PARTITION BY TERR_ID,GEOGRCL_AREA_SHRT_NAME ORDER BY EDW_END_DTTM desc) = 1
);


-- Component LKP_CTRY, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_CTRY AS
(
SELECT CTRY.CTRY_ID as CTRY_ID, CTRY.GEOGRCL_AREA_NAME as GEOGRCL_AREA_NAME, CTRY.GEOGRCL_AREA_DESC as GEOGRCL_AREA_DESC, CTRY.EDW_STRT_DTTM as EDW_STRT_DTTM, CTRY.EDW_END_DTTM as EDW_END_DTTM, CTRY.GEOGRCL_AREA_SHRT_NAME as GEOGRCL_AREA_SHRT_NAME 
FROM db_t_prod_core.CTRY
QUALIFY ROW_NUMBER() OVER(PARTITION BY GEOGRCL_AREA_SHRT_NAME 
ORDER BY EDW_END_DTTM desc) = 1
/* WHERE CTRY.EDW_END_DTTM=TO_DATE(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'') */
);


-- Component LKP_DWLNG_TYPE, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_DWLNG_TYPE AS
(
SELECT
DWLNG_TYPE_CD,
DWLNG_TYPE_DESC
FROM db_t_prod_core.DWLNG_TYPE
);


-- Component LKP_POSTALCODE, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_POSTALCODE AS
(
SELECT POSTL_CD.POSTL_CD_ID as POSTL_CD_ID, POSTL_CD.CTRY_ID as CTRY_ID, POSTL_CD.POSTL_CD_NUM as POSTL_CD_NUM 
FROM db_t_prod_core.POSTL_CD 
QUALIFY ROW_NUMBER() OVER(PARTITION BY CTRY_ID, POSTL_CD_NUM  
ORDER BY EDW_END_DTTM desc) = 1
);


-- Component LKP_SPATIALPOINT_PL, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_SPATIALPOINT_PL AS
(
Select	
X.SpatialPointInternal As SpatialPointInternal,
X.AddressLine1Internal As AddressLine1Internal,
X.AddressLine2Internal As AddressLine2Internal,
X.AddressLine3Internal As AddressLine3Internal,
LKP_CTRY.CTRY_ID As TG_CTRY_ID,
LKP_TERR.TERR_ID As TG_TERR_ID,
LKP_CITY.CITY_ID AS TG_CITY_ID,	
LKP_POSTL.POSTL_CD_ID AS TG_POSTL_CD_ID,
LKP_CNTY.CNTY_ID AS TG_CNTY_ID
FROM (
SELECT	
pctl_state.typecode_stg AS state_TYPECODE,		
pctl_country.typecode_stg AS ctry_TYPECODE ,		
COALESCE(pc_policylocation.CountyInternal_stg ,''US'') AS County,		
pc_policylocation.PostalCodeInternal_stg as PostalCode, 
pc_policylocation.CityInternal_stg AS City, 
pc_policylocation.AddressLine1Internal_stg AS AddressLine1Internal,		
pc_policylocation.AddressLine2Internal_stg AS AddressLine2Internal,		
pc_policylocation.AddressLine3Internal_stg AS AddressLine3Internal,		
Cast(pc_policylocation.SpatialPointInternal_stg AS varchar(100)) as SpatialPointInternal 
FROM db_t_prod_stag.pc_policylocation 
LEFT OUTER JOIN db_t_prod_stag.pctl_state 
ON pc_policylocation.StateInternal_stg = pctl_state.id_stg 
LEFT OUTER JOIN db_t_prod_stag.pctl_country 
ON pctl_country.id_stg = pc_policylocation.CountryInternal_stg 
WHERE	  pc_policylocation.UpdateTime_stg> (:start_dttm)
and pc_policylocation.UpdateTime_stg <= (:end_dttm) and	
state_typecode IS NOT NULL 
AND postalcode  IS NOT NULL 
AND city  IS NOT NULL
and pc_policylocation.SpatialPointInternal_stg is Not NULL )x 
LEFT OUTER JOIN 
(
SELECT	CTRY_ID , GEOGRCL_AREA_NAME, GEOGRCL_AREA_DESC , EDW_STRT_DTTM ,
EDW_END_DTTM , GEOGRCL_AREA_SHRT_NAME 
FROM	 db_t_prod_core.CTRY
QUALIFY	ROW_NUMBER() OVER(PARTITION BY GEOGRCL_AREA_SHRT_NAME 
ORDER	BY EDW_END_DTTM desc) = 1 ) LKP_CTRY
ON	X.ctry_TYPECODE = LKP_CTRY.GEOGRCL_AREA_SHRT_NAME
LEFT OUTER JOIN
(
SELECT	TERR_ID , GEOGRCL_AREA_NAME, GEOGRCL_AREA_DESC , GEOGRCL_AREA_STRT_DTTM ,
GEOGRCL_AREA_END_DTTM , LOCTR_SBTYPE_CD , GEOGRCL_AREA_SBTYPE_CD ,
EDW_STRT_DTTM , EDW_END_DTTM , CTRY_ID , GEOGRCL_AREA_SHRT_NAME 
FROM	 db_t_prod_core.TERR
QUALIFY	ROW_NUMBER () OVER (PARTITION BY CTRY_ID,GEOGRCL_AREA_SHRT_NAME 
ORDER	BY edw_end_dttm DESC)=1) LKP_TERR
ON	LKP_CTRY.CTRY_ID = LKP_TERR. CTRY_ID
AND	X.state_TYPECODE = LKP_TERR.GEOGRCL_AREA_SHRT_NAME
LEFT OUTER JOIN
(
SELECT	CITY_ID, EDW_STRT_DTTM, EDW_END_DTTM , TERR_ID , GEOGRCL_AREA_SHRT_NAME 
FROM	 db_t_prod_core.CITY 
QUALIFY	ROW_NUMBER() OVER(PARTITION BY TERR_ID, GEOGRCL_AREA_SHRT_NAME 
ORDER	BY EDW_END_DTTM desc) = 1) LKP_CITY
ON	LKP_TERR.TERR_ID = LKP_CITY. TERR_ID
AND	X.CITY = LKP_CITY.GEOGRCL_AREA_SHRT_NAME
LEFT OUTER JOIN
( 
SELECT	POSTL_CD_ID, CTRY_ID , POSTL_CD_NUM 
FROM	 db_t_prod_core.POSTL_CD 
QUALIFY	ROW_NUMBER() OVER(PARTITION BY CTRY_ID, POSTL_CD_NUM 
ORDER	BY EDW_END_DTTM desc) = 1) LKP_POSTL
ON	LKP_CTRY.CTRY_ID = LKP_POSTL.CTRY_ID
AND	X.postalcode = LKP_POSTL. POSTL_CD_NUM
LEFT OUTER JOIN
(
SELECT	CNTY_ID , TERR_ID , GEOGRCL_AREA_SBTYPE_CD 
,GEOGRCL_AREA_SHRT_NAME
,LOCTR_SBTYPE_CD ,
EDW_END_DTTM as EDW_END_DTTM 
FROM	 db_t_prod_core.CNTY
QUALIFY	ROW_NUMBER() OVER(PARTITION BY TERR_ID,GEOGRCL_AREA_SHRT_NAME 
ORDER	BY EDW_END_DTTM desc) = 1) LKP_CNTY
ON	LKP_TERR.TERR_ID = LKP_CNTY.TERR_ID
AND	X.county = LKP_CNTY.GEOGRCL_AREA_SHRT_NAME
);


-- Component LKP_TAX_LOC, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TAX_LOC AS
(
SELECT
TAX_LOC_ID,
CITY_ID,
GEOGRCL_SHRT_NAME,
TERR_ID
FROM db_t_prod_core.TAX_LOC
);


-- Component LKP_TERADATA_ETL_REF_XLAT_ADDRESS_SBTYPE, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_ADDRESS_SBTYPE AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	db_t_prod_core.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''ADDR_SBTYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_ADDRESS_STANDARD_CD, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_ADDRESS_STANDARD_CD AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	db_t_prod_core.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''ADDR_STDZN_TYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM in(''pctl_addressstandardtype_alfa.typecode'',''bctl_addressstandardtype_alfa.typecode'',''cctl_addressstandardtype_alfa.typecode'')

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''GW'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_GEO_STATUS_CD, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_GEO_STATUS_CD AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	db_t_prod_core.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''GEOCODE_STS_TYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM in (''pctl_geocodestatus.typecode'',''bctl_geocodestatus.typecode'',''cctl_geocodestatus.typecode'') 		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''GW'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_LOCTR_SBTYPE, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_LOCTR_SBTYPE AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	db_t_prod_core.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''LOCTR_SBTYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERR, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERR AS
(
SELECT TERR.TERR_ID as TERR_ID, TERR.GEOGRCL_AREA_NAME as GEOGRCL_AREA_NAME, TERR.GEOGRCL_AREA_DESC as GEOGRCL_AREA_DESC, TERR.GEOGRCL_AREA_STRT_DTTM as GEOGRCL_AREA_STRT_DTTM, TERR.GEOGRCL_AREA_END_DTTM as GEOGRCL_AREA_END_DTTM, TERR.LOCTR_SBTYPE_CD as LOCTR_SBTYPE_CD, TERR.GEOGRCL_AREA_SBTYPE_CD as GEOGRCL_AREA_SBTYPE_CD, TERR.EDW_STRT_DTTM as EDW_STRT_DTTM, TERR.EDW_END_DTTM as EDW_END_DTTM, TERR.CTRY_ID as CTRY_ID, TERR.GEOGRCL_AREA_SHRT_NAME as GEOGRCL_AREA_SHRT_NAME FROM db_t_prod_core.TERR
QUALIFY ROW_NUMBER () OVER (PARTITION BY CTRY_ID,GEOGRCL_AREA_SHRT_NAME ORDER BY edw_end_dttm DESC)=1
);


-- Component sq_cc_address, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_cc_address AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as state_TYPECODE,
$2 as ctry_TYPECODE,
$3 as County,
$4 as PostalCode,
$5 as City,
$6 as AddressLine1,
$7 as AddressLine2,
$8 as AddressLine3,
$9 as Dwelling_Type_Cd,
$10 as spatialpoint,
$11 as Retired,
$12 as Geocode_Status_Cd,
$13 as Address_Standard_cd,
$14 as tax_city,
$15 as CODE,
$16 as tax_state,
$17 as Latitude,
$18 as Longitude,
$19 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select state_TYPECODE_stg,										

		ctry_TYPECODE_stg ,								

  		county_stg  COLLATE ''en-ci'' , 								

        postalcode_stg, 										

        city_stg COLLATE ''en-ci'' , 										

        addressline1_stg COLLATE ''en-ci'' , 										

        addressline2_stg COLLATE ''en-ci'' ,  										

        addressline3_stg COLLATE ''en-ci'' , 										

      Dwelling_Type_Cd_stg, 										

      spatialpoint_stg,										

      Retired_stg      ,										

		Geocode_Status_Cd_stg,								

		Address_Standard_cd_stg ,tax_city_stg,code_stg,tax_state_stg,	latitude_stg, longitude_stg from							

		(SELECT  distinct state_TYPECODE_stg,								

		case when ctry_TYPECODE_stg is null then ''US'' else ctry_TYPECODE_stg end ctry_TYPECODE_stg,								

  		county_stg, 								

        postalcode_stg, 										

        city_stg, 										

        addressline1_stg, 										

        addressline2_stg, 										

        addressline3_stg,										

      Dwelling_Type_Cd_stg, 										

      spatialpoint_stg,										

      Retired_stg      ,										

		Geocode_Status_Cd_stg,								

		Address_Standard_cd_stg   ,								

		tax_city_stg,								

		code_stg,								

		tax_state_stg,								

		updatetime_stg,								

		 	latitude_stg,							

		 longitude_stg								

FROM   (										

SELECT 										

        bctl_state.typecode_stg                           AS state_TYPECODE_stg, 										

        bctl_country.typecode_stg                         AS ctry_TYPECODE_stg, 										

        bc_address.county_stg, 										

        bc_address.postalcode_stg, 										

        bc_address.city_stg, 										

        bc_address.addressline1_stg, 										

        bc_address.addressline2_stg, 										

        bc_address.addressline3_stg,										

		bc_address.updatetime_stg  ,								

        contact.typecode_stg                              AS Dwelling_Type_Cd_stg, 										

        Cast(bc_address.spatialpoint_stg AS VARCHAR(100)) AS spatialpoint_stg ,										

        bc_address.retired_stg,										

		 Geocode_Status_Cd_stg,								

		Address_Standard_cd_stg,								

		Cast(null  AS varchar(100))  as tax_city_stg,								

		Cast(null AS varchar(100))  as code_stg,								

		Cast(null AS varchar(100))  as tax_state_stg,								

		cast(bc_address.latitude_stg as varchar(25)) as 	latitude_stg,							

		cast(bc_address.longitude_stg as varchar(25)) as longitude_stg								

 										

 FROM   (select  bc_address.county_Stg, 										

        bc_address.postalcode_stg, 										

        bc_address.city_stg, 										

        bc_address.addressline1_stg, 										

        bc_address.addressline2_stg, 										

        bc_address.addressline3_stg,										

		bc_address.updatetime_stg,								

		bc_address.spatialpoint_stg,								

		bc_address.retired_stg,								

bc_address.id_stg,										

bctl_addressstandardtype_alfa.TYPECODE_stg as Address_Standard_cd_stg ,										

bctl_geocodestatus.TYPECODE_stg as 	Geocode_Status_Cd_stg,bc_address.state_stg,									

bc_address.country_stg, bc_address.latitude_stg,										

bc_address.longitude_stg										

FROM										

   DB_T_PROD_STAG.bc_address										

										

left join   DB_T_PROD_STAG.bctl_geocodestatus on bc_address.geocodestatus_stg = bctl_geocodestatus.id_stg										

left join   DB_T_PROD_STAG.bctl_addressstandardtype_alfa on bc_address.StandardizationType_alfa_stg = bctl_addressstandardtype_alfa.id_stg										

WHERE bc_address.UpdateTime_stg>(:start_dttm) AND bc_address.UpdateTime_stg <= (:end_dttm) 										

) bc_address 										

               LEFT OUTER JOIN  DB_T_PROD_STAG.bctl_state 										

                            ON bc_address.state_stg = bctl_state.id_stg 										

               LEFT OUTER JOIN  DB_T_PROD_STAG.bctl_country 										

                            ON bctl_country.id_stg = bc_address.country_stg 										

               LEFT JOIN (SELECT DISTINCT primaryaddressid_stg, 										

                                          bctl_contact.typecode_stg 										

                          FROM   ( 										

						 select subtype_stg,a.primaryaddressid_stg				

						  FROM  DB_T_PROD_STAG.bc_contact a				

WHERE 										

a.UpdateTime_stg > (:start_dttm)										

and a.UpdateTime_stg <= (:end_dttm) )bc_contact1 										

                                 INNER JOIN  DB_T_PROD_STAG.bctl_contact 										

                                         ON bc_contact1.subtype_stg = bctl_contact.id_stg 										

                                            AND 										

                                 bctl_contact.typecode_stg = ''LegalVenue'') 										

                         contact 										

                      ON contact.primaryaddressid_stg = bc_address.id_stg										

										

 										

/*========================================================cc ==================================================================*/										

UNION										

SELECT 										

/* cc_address.State, cc_address.Country, cctl_country.ID AS ctry_ID,    cctl_state.ID AS state_ID, 										 */
        cctl_state.typecode_stg                           AS state_TYPECODE, 										

        cctl_country.typecode_stg                         AS ctry_TYPECODE, 										

        cc_address.County_stg, 										

        cc_address.postalcode_stg, 										

        cc_address.city_stg, 										

        cc_address.addressline1_stg, 										

        cc_address.addressline2_stg, 										

        cc_address.addressline3_stg, cc_address.updatetime_stg as updatetime,										

        contact.typecode_stg                              AS Dwelling_Type_Cd, 										

        Cast(cc_address.spatialpoint_stg AS VARCHAR(100)) AS spatialpoint ,										

        cc_address.retired_stg,										

		Geocode_Status_Cd_stg,								

		Address_Standard_cd_stg,								

		Cast(null  AS varchar(100))  as tax_city,								

		Cast(null AS varchar(100))  as code,								

		Cast(null AS varchar(100))  as tax_state,								

		cast(cc_address.latitude_stg as varchar(25)) as 	latitude_stg,							

		cast(cc_address.longitude_stg as varchar(25)) as longitude_stg								

        FROM 										

		(  SELECT 								

		cc_address.County_stg,								

		cc_address.PostalCode_stg,              								

		cc_address.City_stg,								

		cc_address.AddressLine1_stg,								

		cc_address.AddressLine2_stg,								

		cc_address.addressline3_stg, 								

		cc_address.updatetime_stg, 								

		cc_address.SpatialPoint_stg,								

		cc_address.Retired_stg,								

		cc_address.Subtype_stg,								

		cctl_geocodestatus.typecode_stg as Geocode_Status_Cd_stg,								

		cctl_addressstandardtype_alfa.typecode_stg as Address_Standard_cd_stg,								

		cc_address.State_stg,								

		cc_address.Country_stg,								

		cc_address.ID_stg,cc_address.latitude_stg,								

cc_address.longitude_stg										

		FROM  DB_T_PROD_STAG.Cc_address								

left join  DB_T_PROD_STAG.cctl_geocodestatus on cc_address.geocodestatus_stg = cctl_geocodestatus.id_stg										

left join  DB_T_PROD_STAG.cctl_addressstandardtype_alfa on cc_address.StandardizationType_alfa_stg = cctl_addressstandardtype_alfa.id_stg										

WHERE Cc_address.UpdateTime_stg>(:start_dttm) 										

AND Cc_address.UpdateTime_stg <= (:end_dttm)) 										

Cc_address 										

               LEFT OUTER JOIN DB_T_PROD_STAG.cctl_state 										

                            ON cc_address.state_stg = cctl_state.id_stg 										

               LEFT OUTER JOIN DB_T_PROD_STAG.cctl_country 										

                            ON cctl_country.id_stg = cc_address.country_stg 										

               LEFT JOIN (SELECT DISTINCT cc_contact.primaryaddressid_stg, 										

                                          cctl_contact.typecode_stg 										

                          FROM  ( select primaryaddressid_stg ,subtype_stg from DB_T_PROD_STAG.cc_contact										

left join DB_T_PROD_STAG.cctl_vendoravailtype_alfa  on 										

cc_contact.VendorAvailability_alfa_stg = cctl_vendoravailtype_alfa.ID_stg										

where cc_contact.UpdateTime_stg > (:start_dttm) 										

AND cc_contact.UpdateTime_stg <= (:end_dttm)) cc_contact 										

                                 INNER JOIN DB_T_PROD_STAG.cctl_contact 										

                                         ON cc_contact.subtype_stg = cctl_contact.id_stg 										

                                            AND 										

                                 cctl_contact.typecode_stg = ''LegalVenue'') 										

                         contact 										

                      ON contact.primaryaddressid_stg = cc_address.id_stg 										

/*========================================================pc==================================================================*/										

										

UNION										

SELECT 										

             										

/* pc_address.id,pc_address.State, pc_address.Country, pctl_country.ID AS ctry_ID,    pctl_state.ID AS state_ID, 										 */
        pctl_state.typecode_stg                           AS state_TYPECODE, 										

        pctl_country.typecode_stg                         AS ctry_TYPECODE, 										

        pc_address.county_stg, 										

        pc_address.postalcode_stg, 										

        pc_address.city_stg, 										

        pc_address.addressline1_stg, 										

        pc_address.addressline2_stg, 										

        pc_address.addressline3_stg, 										

		pc_address.updatetime_stg as updatetime,								

        contact.typecode_stg                              AS Dwelling_Type_Cd, 										

       Cast(pc_address.spatialpoint_stg AS VARCHAR(100)) AS spatialpoint,										

/*    Cast(COALESCE(pc_address.spatialpoint, pc_policylocation.spatialpointinternal) AS VARCHAR(100)) AS spatialpoint,       										 */
        pc_address.Retired_stg ,										

		Geocode_Status_Cd_stg,								

		Address_Standard_cd_stg,								

		pc_taxlocation.city_stg as tax_city ,								

		code_stg,								

		PCTL_JURISDICTION.TYPECODE_stg as tax_state,								

		cast(latitude_stg as varchar(25)) as latitude_stg,								

	cast(longitude_stg as varchar(25)) as longitude_stg									

        FROM   ( 										

		SELECT  								

		pc_address.County_stg,								

		pc_address.PostalCode_stg,              								

		pc_address.City_stg,								

		pc_address.AddressLine1_stg,								

		pc_address.AddressLine2_stg,								

		pc_address.addressline3_stg, 								

		pc_address.updatetime_stg, 								

		pc_address.SpatialPoint_stg,								

		pc_address.Retired_stg,								

		pc_address.Subtype_stg,								

		pctl_geocodestatus.typecode_stg as Geocode_Status_Cd_stg,								

		pctl_addressstandardtype_alfa.typecode_stg as Address_Standard_cd_stg,								

		pc_address.State_stg,								

		pc_address.Country_stg,								

		pc_address.ID_stg,								

cast(pc_address.latitude_stg as varchar(25)) as latitude_stg  , -- as latitude_stg -- added latitude and longitude values */								 */
		cast(pc_address.Longitude_stg as varchar(25))  as Longitude_stg 								

		FROM								

 DB_T_PROD_STAG.pc_address 										

left join DB_T_PROD_STAG.pctl_geocodestatus 										

	on	pc_address.geocodestatus_stg = pctl_geocodestatus.id_stg								

left join DB_T_PROD_STAG.pctl_addressstandardtype_alfa 										

	on	pc_address.standardizedtype_alfa_stg = pctl_addressstandardtype_alfa.id_stg								

WHERE	pc_address.UpdateTime_stg> (:start_dttm)									

	and	pc_address.UpdateTime_stg <= (:end_dttm)) pc_address 								

               LEFT OUTER JOIN DB_T_PROD_STAG.PCTL_STATE as PCTL_STATE										

                            ON pc_address.state_stg = pctl_state.id_stg 										

               LEFT OUTER JOIN DB_T_PROD_STAG.pctl_country 										

                            ON pctl_country.id_stg = pc_address.country_stg 										

               LEFT JOIN (SELECT DISTINCT pc_contact.primaryaddressid_stg, 										

                                          pctl_contact.typecode_stg 										

                          FROM   DB_T_PROD_STAG.pc_contact 										

                                 INNER JOIN DB_T_PROD_STAG.pctl_contact 										

                                         ON pc_contact.subtype_stg = pctl_contact.id_stg 										

                                            AND 										

                                 pctl_contact.typecode_stg = ''LegalVenue''										

								 WHERE pc_contact.UpdateTime_stg> (:start_dttm)		

									and pc_contact.UpdateTime_stg <= (:end_dttm)) 	

                         contact 										

                      ON contact.primaryaddressid_stg = pc_address.id_stg										

                    left  join ( select AccountLocation_stg ,taxlocation_stg from  DB_T_PROD_STAG.pc_policylocation										

					 where  pc_policylocation.UpdateTime_stg> (:start_dttm)					

									and pc_policylocation.UpdateTime_stg <= (:end_dttm))	as pc_policylocation 

									on pc_policylocation.AccountLocation_stg = pc_address.ID_stg	

                    left join  ( SELECT id_stg,STATE_stg,city_stg,CODE_STG FROM DB_T_PROD_STAG.pc_taxlocation 										

					WHERE UpdateTime_STG > (:start_dttm) 					

					AND pc_taxlocation.UpdateTime_STG <= (:end_dttm)) 					

					as pc_taxlocation on pc_policylocation.taxlocation_stg = pc_taxlocation.id_stg					

                   left  JOIN DB_T_PROD_STAG.PCTL_JURISDICTION ON PCTL_JURISDICTION.ID_stg=pc_taxlocation.STATE_stg										

/*==============================================BOP AND CHURCH DB_T_CORE_DM_PROD.POLICY LOCATION====================================*/										

										

UNION 										

										

SELECT 										

        pctl_state.typecode_stg                            AS state_TYPECODE, 										

        pctl_country.typecode_stg                          AS ctry_TYPECODE, 										

        pc_policylocation.CountyInternal_stg 			  AS County, 							

        pc_policylocation.PostalCodeInternal_stg  as PostalCode, 										

        pc_policylocation.CityInternal_stg  AS City, 										

        pc_policylocation.AddressLine1Internal_stg  AS AddressLine1, 										

        pc_policylocation.AddressLine2Internal_stg  AS AddressLine2, 										

        pc_policylocation.AddressLine3Internal_stg  AS AddressLine3,										

		pc_policylocation.updatetime_stg  as updatetime,								

        contact.typecode_stg                              AS Dwelling_Type_Cd, 										

Cast(pc_policylocation.SpatialPointInternal_stg  AS varchar(100))  as spatialpoint,										

        0 ,										

   	 Cast(NULL  AS varchar(100))  AS Geocode_Status_Cd,									

	 Cast(NULL  AS varchar(100)) AS Address_Standard_cd,									

		pc_taxlocation.city_stg as tax_city ,								

		code_stg,								

		PCTL_JURISDICTION.TYPECODE_stg as tax_state,								

			cast(pc_policylocation.latitude_stg as varchar(25)) as 	latitude_stg,						

		cast(pc_policylocation.longitude_stg as varchar(25)) as longitude_stg								

        FROM  										

		(select * FROM  DB_T_PROD_STAG.pc_policylocation 								

		          where  pc_policylocation.UpdateTime_stg> (:start_dttm)								

				and pc_policylocation.UpdateTime_stg <= (:end_dttm)) pc_policylocation						

               LEFT OUTER JOIN DB_T_PROD_STAG.PCTL_STATE  PCTL_STATE										

                            ON pc_policylocation.StateInternal_stg = pctl_state.id_stg 										

               LEFT OUTER JOIN DB_T_PROD_STAG.pctl_country 										

                            ON pctl_country.id_stg = pc_policylocation.CountryInternal_stg 										

               LEFT JOIN (SELECT DISTINCT pc_contact.primaryaddressid_stg, 										

                                          pctl_contact.typecode_stg 										

                          FROM   DB_T_PROD_STAG.pc_contact 										

                                 INNER JOIN DB_T_PROD_STAG.pctl_contact 										

                                         ON pc_contact.subtype_stg = pctl_contact.id_stg 										

                                            AND 										

                                 pctl_contact.typecode_stg = ''LegalVenue''										

								 WHERE pc_contact.UpdateTime_stg> (:start_dttm)		

and pc_contact.UpdateTime_stg <= (:end_dttm)) 										

                         contact 										

                      ON contact.primaryaddressid_stg = pc_policylocation.id_stg										

                    left  join ( select AccountLocation_stg ,taxlocation_stg from DB_T_PROD_STAG.pc_policylocation										

					 where  pc_policylocation.UpdateTime_stg> (:start_dttm)					

									and pc_policylocation.UpdateTime_stg <= (:end_dttm))	as pco 

									on pco.AccountLocation_stg = pc_policylocation.ID_stg	

                    left join ( SELECT id_stg,STATE_stg,city_stg,CODE_STG FROM DB_T_PROD_STAG.pc_taxlocation 										

					WHERE UpdateTime_STG > (:start_dttm) 					

					AND pc_taxlocation.UpdateTime_STG <= (:end_dttm)) 					

					as pc_taxlocation on pc_policylocation.taxlocation_stg = pc_taxlocation.id_stg					

                   left  JOIN DB_T_PROD_STAG.PCTL_JURISDICTION ON PCTL_JURISDICTION.ID_stg=pc_taxlocation.STATE_stg										

 										

										

										

/* add from here */										

                                            )  t1										

WHERE   state_typecode_stg IS NOT NULL 										

       AND postalcode_stg IS NOT NULL 										

       AND city_stg IS NOT NULL )x  										

	   									

										

		QUALIFY ROW_NUMBER ()  Over ( Partition by state_TYPECODE_stg,ctry_TYPECODE_stg,county_stg COLLATE ''en-ci'' ,postalcode_stg, city_stg COLLATE ''en-ci'' ,addressline1_stg COLLATE ''en-ci'' , addressline2_stg COLLATE ''en-ci'' , addressline3_stg COLLATE ''en-ci''  								

										

Order by Dwelling_Type_Cd_stg desc,spatialpoint_stg  desc nulls last,latitude_stg desc nulls last, longitude_stg desc nulls last, updatetime_stg desc nulls last,  Retired_stg desc,Geocode_Status_Cd_stg desc nulls last,										

										

        Address_Standard_cd_stg desc nulls last , tax_city_stg desc nulls last, code_stg desc nulls last,										

										

        tax_state_stg desc nulls last)=1
) SRC
)
);


-- Component exp_pass_from_source_pc1111, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_from_source_pc1111 AS
(
SELECT
sq_cc_address.state_TYPECODE as state_TYPECODE,
sq_cc_address.ctry_TYPECODE as ctry_TYPECODE,
sq_cc_address.County as County,
sq_cc_address.PostalCode as PostalCode,
sq_cc_address.City as City,
sq_cc_address.AddressLine1 as AddressLine1,
sq_cc_address.AddressLine2 as AddressLine2,
sq_cc_address.AddressLine3 as AddressLine3,
sq_cc_address.Dwelling_Type_Cd as Dwelling_Type_Cd,
sq_cc_address.spatialpoint as spatialpoint,
sq_cc_address.Retired as Retired,
sq_cc_address.Geocode_Status_Cd as Geocode_Status_Cd,
sq_cc_address.Address_Standard_cd as Address_Standard_cd,
sq_cc_address.tax_city as tax_city,
sq_cc_address.CODE as CODE,
sq_cc_address.tax_state as tax_state,
sq_cc_address.Latitude as Latitude,
sq_cc_address.Longitude as Longitude,
sq_cc_address.source_record_id
FROM
sq_cc_address
);


-- Component exp_data_transformation, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_data_transformation AS
(
SELECT
exp_pass_from_source_pc1111.AddressLine1 as AddressLine1,
exp_pass_from_source_pc1111.AddressLine2 as AddressLine2,
exp_pass_from_source_pc1111.AddressLine3 as AddressLine3,
exp_pass_from_source_pc1111.spatialpoint as spatialpoint,
LKP_1.CTRY_ID /* replaced lookup LKP_CTRY */ as v_ctry_id,
LKP_2.TERR_ID /* replaced lookup LKP_TERR */ as v_terr_id,
LKP_3.TERR_ID /* replaced lookup LKP_TERR */ as v_terr_id_tax,
LKP_4.CITY_ID /* replaced lookup LKP_CITY */ as v_city_id_tax,
LKP_5.CNTY_ID /* replaced lookup LKP_COUNTY */ as v_cnty_id,
LKP_6.POSTL_CD_ID /* replaced lookup LKP_POSTALCODE */ as v_postl_cd_id,
LKP_7.CITY_ID /* replaced lookup LKP_CITY */ as v_city_id,
CASE WHEN LKP_8.DWLNG_TYPE_CD /* replaced lookup LKP_DWLNG_TYPE */ IS NULL THEN ''UNK'' ELSE LKP_9.DWLNG_TYPE_CD /* replaced lookup LKP_DWLNG_TYPE */ END as v_Dwelling_Type_Cd,
--v_street_addr_id,  -- missing derivation for v_street_addr_id
''LOCTR_SBTYPE1'' as v_loctr_sbtype_val,
''ADDR_SBTYPE1'' as v_address_sbtype_val,
LKP_10.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_LOCTR_SBTYPE */ as v_loctr_sbtype,
LKP_11.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ADDRESS_SBTYPE */ as v_address_sbtype,
CASE WHEN NULL IS NULL THEN 1 ELSE 0 END as V_UPD_OR_INS,
v_ctry_id as out_ctry_id,
LKP_12.TAX_LOC_ID /* replaced lookup LKP_TAX_LOC */ as out_TAX_LOC_ID,
v_cnty_id as out_cnty_id,
v_terr_id as out_terr_id,
v_city_id as out_city_id,
v_postl_cd_id as out_postl_cd_id,
v_Dwelling_Type_Cd as out_Dwelling_Type_Cd,
v_loctr_sbtype as out_loctr_sbtype,
v_address_sbtype as out_address_sbtype,
NULL as out_street_addr_id,
:PRCS_ID as out_process_id,
exp_pass_from_source_pc1111.Retired as Retired,
CASE WHEN LKP_13.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_GEO_STATUS_CD */ IS NULL THEN ''UNK'' ELSE LKP_14.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_GEO_STATUS_CD */ END as out_Geocode_Status_Cd,
CASE WHEN LKP_15.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ADDRESS_STANDARD_CD */ IS NULL THEN ''UNK'' ELSE LKP_16.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ADDRESS_STANDARD_CD */ END as out_Address_Standard_cd,
IFNULL(TRY_TO_DECIMAL(LTRIM ( RTRIM ( exp_pass_from_source_pc1111.Latitude ) )), 0) as out_Latitude,
IFNULL(TRY_TO_DECIMAL(LTRIM ( RTRIM ( exp_pass_from_source_pc1111.Longitude ) )), 0) as out_Longitude,
exp_pass_from_source_pc1111.source_record_id,
row_number() over (partition by exp_pass_from_source_pc1111.source_record_id order by exp_pass_from_source_pc1111.source_record_id) as RNK
FROM
exp_pass_from_source_pc1111
LEFT JOIN LKP_CTRY LKP_1 ON LKP_1.GEOGRCL_AREA_SHRT_NAME = exp_pass_from_source_pc1111.ctry_TYPECODE
LEFT JOIN LKP_TERR LKP_2 ON LKP_2.CTRY_ID = v_ctry_id AND LKP_2.GEOGRCL_AREA_SHRT_NAME = exp_pass_from_source_pc1111.state_TYPECODE
LEFT JOIN LKP_TERR LKP_3 ON LKP_3.CTRY_ID = v_ctry_id AND LKP_3.GEOGRCL_AREA_SHRT_NAME = exp_pass_from_source_pc1111.tax_state
LEFT JOIN LKP_CITY LKP_4 ON LKP_4.TERR_ID = v_terr_id_tax AND LKP_4.GEOGRCL_AREA_SHRT_NAME = exp_pass_from_source_pc1111.tax_city
LEFT JOIN LKP_COUNTY LKP_5 ON LKP_5.TERR_ID = v_terr_id AND LKP_5.GEOGRCL_AREA_SHRT_NAME = ltrim ( rtrim ( exp_pass_from_source_pc1111.County ) )
LEFT JOIN LKP_POSTALCODE LKP_6 ON LKP_6.CTRY_ID = v_ctry_id AND LKP_6.POSTL_CD_NUM = exp_pass_from_source_pc1111.PostalCode
LEFT JOIN LKP_CITY LKP_7 ON LKP_7.TERR_ID = v_terr_id AND LKP_7.GEOGRCL_AREA_SHRT_NAME = exp_pass_from_source_pc1111.City
LEFT JOIN LKP_DWLNG_TYPE LKP_8 ON LKP_8.DWLNG_TYPE_CD = exp_pass_from_source_pc1111.Dwelling_Type_Cd
LEFT JOIN LKP_DWLNG_TYPE LKP_9 ON LKP_9.DWLNG_TYPE_CD = exp_pass_from_source_pc1111.Dwelling_Type_Cd
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_LOCTR_SBTYPE LKP_10 ON LKP_10.SRC_IDNTFTN_VAL = v_loctr_sbtype_val
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ADDRESS_SBTYPE LKP_11 ON LKP_11.SRC_IDNTFTN_VAL = v_address_sbtype_val
LEFT JOIN LKP_TAX_LOC LKP_12 ON LKP_12.CITY_ID = v_city_id_tax AND LKP_12.GEOGRCL_SHRT_NAME = exp_pass_from_source_pc1111.CODE AND LKP_12.TERR_ID = v_terr_id_tax
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_GEO_STATUS_CD LKP_13 ON LKP_13.SRC_IDNTFTN_VAL = exp_pass_from_source_pc1111.Geocode_Status_Cd
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_GEO_STATUS_CD LKP_14 ON LKP_14.SRC_IDNTFTN_VAL = exp_pass_from_source_pc1111.Geocode_Status_Cd
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ADDRESS_STANDARD_CD LKP_15 ON LKP_15.SRC_IDNTFTN_VAL = exp_pass_from_source_pc1111.Address_Standard_cd
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ADDRESS_STANDARD_CD LKP_16 ON LKP_16.SRC_IDNTFTN_VAL = exp_pass_from_source_pc1111.Address_Standard_cd
QUALIFY RNK = 1
);


-- Component exp_SrcFields, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_SrcFields AS
(
SELECT
exp_data_transformation.out_street_addr_id as in_STREET_ADDR_ID_UPD,
exp_data_transformation.AddressLine1 as in_ADDR_LN_1_TXT,
exp_data_transformation.AddressLine2 as in_ADDR_LN_2_TXT,
exp_data_transformation.AddressLine3 as in_ADDR_LN_3_TXT,
exp_data_transformation.out_Dwelling_Type_Cd as in_DWLNG_TYPE_CD,
exp_data_transformation.out_city_id as in_CITY_ID,
exp_data_transformation.out_terr_id as in_TERR_ID,
exp_data_transformation.out_postl_cd_id as in_POSTL_CD_ID,
exp_data_transformation.out_ctry_id as in_CTRY_ID,
exp_data_transformation.out_cnty_id as in_CNTY_ID,
CASE WHEN exp_data_transformation.spatialpoint IS NULL THEN LKP_1.SpatialPointInternal /* replaced lookup LKP_SPATIALPOINT_PL */ ELSE exp_data_transformation.spatialpoint END as v_SPL_PNT,
v_SPL_PNT as out_SPL_PNT,
exp_data_transformation.out_process_id as in_PRCS_ID,
exp_data_transformation.out_loctr_sbtype as in_LOCATR_SBTYPE_CD,
exp_data_transformation.out_address_sbtype as in_ADDR_SBTYPE_CD,
exp_data_transformation.Retired as Retired,
exp_data_transformation.out_Geocode_Status_Cd as out_Geocode_Status_Cd,
exp_data_transformation.out_Address_Standard_cd as out_Address_Standard_cd,
exp_data_transformation.out_TAX_LOC_ID as out_TAX_LOC_ID,
exp_data_transformation.out_Latitude as out_Latitude,
exp_data_transformation.out_Longitude as out_Longitude,
exp_data_transformation.source_record_id,
row_number() over (partition by exp_data_transformation.source_record_id order by exp_data_transformation.source_record_id) as RNK
FROM
exp_data_transformation
LEFT JOIN LKP_SPATIALPOINT_PL LKP_1 ON LKP_1.AddressLine1Internal = exp_data_transformation.AddressLine1 AND LKP_1.AddressLine2Internal = exp_data_transformation.AddressLine2 AND LKP_1.AddressLine3Internal = exp_data_transformation.AddressLine3 AND LKP_1.TG_CTRY_ID = exp_data_transformation.out_ctry_id AND LKP_1.TG_TERR_ID = exp_data_transformation.out_terr_id AND LKP_1.TG_CITY_ID = exp_data_transformation.out_city_id AND LKP_1.TG_POSTL_CD_ID = exp_data_transformation.out_postl_cd_id AND LKP_1.TG_CNTY_ID = exp_data_transformation.out_cnty_id
QUALIFY RNK = 1
);


-- Component LKP_STREETADDR, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_STREETADDR AS
(
SELECT
LKP.STREET_ADDR_ID,
LKP.ADDR_LN_1_TXT,
LKP.ADDR_LN_2_TXT,
LKP.ADDR_LN_3_TXT,
LKP.DWLNG_TYPE_CD,
LKP.CITY_ID,
LKP.TERR_ID,
LKP.POSTL_CD_ID,
LKP.TAX_LOC_ID,
LKP.CTRY_ID,
LKP.CNTY_ID,
LKP.SPTL_PNT,
LKP.LAT_MEAS,
LKP.LNGTD_MEAS,
LKP.LOCTR_SBTYPE_CD,
LKP.ADDR_SBTYPE_CD,
LKP.GEOCODE_STS_TYPE_CD,
LKP.ADDR_STDZN_TYPE_CD,
LKP.PRCS_ID,
LKP.EDW_STRT_DTTM,
LKP.EDW_END_DTTM,
exp_SrcFields.in_ADDR_LN_1_TXT as in_ADDR_LN_1_TXT,
exp_SrcFields.in_ADDR_LN_2_TXT as in_ADDR_LN_2_TXT,
exp_SrcFields.in_ADDR_LN_3_TXT as in_ADDR_LN_3_TXT,
exp_SrcFields.in_CITY_ID as in_CITY_ID,
exp_SrcFields.in_TERR_ID as in_TERR_ID,
exp_SrcFields.in_POSTL_CD_ID as in_POSTL_CD_ID,
exp_SrcFields.in_CTRY_ID as in_CTRY_ID,
exp_SrcFields.in_CNTY_ID as in_CNTY_ID,
exp_SrcFields.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_SrcFields.source_record_id ORDER BY LKP.STREET_ADDR_ID asc,LKP.ADDR_LN_1_TXT asc,LKP.ADDR_LN_2_TXT asc,LKP.ADDR_LN_3_TXT asc,LKP.DWLNG_TYPE_CD asc,LKP.CITY_ID asc,LKP.TERR_ID asc,LKP.POSTL_CD_ID asc,LKP.TAX_LOC_ID asc,LKP.CTRY_ID asc,LKP.CARIER_RTE_TXT asc,LKP.CNTY_ID asc,LKP.SPTL_PNT asc,LKP.LAT_MEAS asc,LKP.LNGTD_MEAS asc,LKP.LOCTR_SBTYPE_CD asc,LKP.ADDR_SBTYPE_CD asc,LKP.GEOCODE_STS_TYPE_CD asc,LKP.ADDR_STDZN_TYPE_CD asc,LKP.PRCS_ID asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc) RNK
FROM
exp_SrcFields
LEFT JOIN (
SELECT STREET_ADDR.STREET_ADDR_ID as STREET_ADDR_ID, STREET_ADDR.DWLNG_TYPE_CD as DWLNG_TYPE_CD, STREET_ADDR.TAX_LOC_ID as TAX_LOC_ID, STREET_ADDR.CARIER_RTE_TXT as CARIER_RTE_TXT, STREET_ADDR.SPTL_PNT as SPTL_PNT, STREET_ADDR.LOCTR_SBTYPE_CD as LOCTR_SBTYPE_CD, STREET_ADDR.ADDR_SBTYPE_CD as ADDR_SBTYPE_CD, STREET_ADDR.GEOCODE_STS_TYPE_CD as GEOCODE_STS_TYPE_CD, STREET_ADDR.ADDR_STDZN_TYPE_CD as ADDR_STDZN_TYPE_CD, STREET_ADDR.PRCS_ID as PRCS_ID, STREET_ADDR.EDW_STRT_DTTM as EDW_STRT_DTTM, STREET_ADDR.EDW_END_DTTM as EDW_END_DTTM, STREET_ADDR.ADDR_LN_1_TXT as ADDR_LN_1_TXT, STREET_ADDR.ADDR_LN_2_TXT as ADDR_LN_2_TXT, STREET_ADDR.ADDR_LN_3_TXT as ADDR_LN_3_TXT, STREET_ADDR.CITY_ID as CITY_ID, STREET_ADDR.TERR_ID as TERR_ID, STREET_ADDR.POSTL_CD_ID as POSTL_CD_ID, STREET_ADDR.CTRY_ID as CTRY_ID, STREET_ADDR.CNTY_ID as CNTY_ID,STREET_ADDR.LAT_MEAS as LAT_MEAS,STREET_ADDR.LNGTD_MEAS as LNGTD_MEAS  FROM db_t_prod_core.STREET_ADDR qualify row_number () over (partition by ADDR_LN_1_TXT,ADDR_LN_2_TXT,ADDR_LN_3_TXT, CITY_ID ,TERR_ID,POSTL_CD_ID,CTRY_ID ,CNTY_ID order by EDW_END_DTTM desc)=1
) LKP ON LKP.ADDR_LN_1_TXT = exp_SrcFields.in_ADDR_LN_1_TXT AND LKP.ADDR_LN_2_TXT = exp_SrcFields.in_ADDR_LN_2_TXT AND LKP.ADDR_LN_3_TXT = exp_SrcFields.in_ADDR_LN_3_TXT AND LKP.CITY_ID = exp_SrcFields.in_CITY_ID AND LKP.TERR_ID = exp_SrcFields.in_TERR_ID AND LKP.POSTL_CD_ID = exp_SrcFields.in_POSTL_CD_ID AND LKP.CTRY_ID = exp_SrcFields.in_CTRY_ID AND LKP.CNTY_ID = exp_SrcFields.in_CNTY_ID
QUALIFY RNK = 1
);


-- Component exp_CDC_Check, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_CDC_Check AS
(
SELECT
exp_SrcFields.in_STREET_ADDR_ID_UPD as in_STREET_ADDR_ID_UPD,
exp_SrcFields.in_ADDR_LN_1_TXT as in_ADDR_LN_1_TXT,
exp_SrcFields.in_ADDR_LN_2_TXT as in_ADDR_LN_2_TXT,
exp_SrcFields.in_ADDR_LN_3_TXT as in_ADDR_LN_3_TXT,
exp_SrcFields.in_DWLNG_TYPE_CD as in_DWLNG_TYPE_CD,
exp_SrcFields.in_CITY_ID as in_CITY_ID,
exp_SrcFields.in_TERR_ID as in_TERR_ID,
exp_SrcFields.in_POSTL_CD_ID as in_POSTL_CD_ID,
exp_SrcFields.in_CTRY_ID as in_CTRY_ID,
exp_SrcFields.in_CNTY_ID as in_CNTY_ID,
CASE WHEN ltrim ( rtrim ( exp_SrcFields.out_SPL_PNT ) ) IS NULL and ltrim ( rtrim ( LKP_STREETADDR.SPTL_PNT ) ) != NULL THEN ltrim ( rtrim ( LKP_STREETADDR.SPTL_PNT ) ) ELSE ltrim ( rtrim ( exp_SrcFields.out_SPL_PNT ) ) END as out_SPL_PNT,
exp_SrcFields.in_PRCS_ID as in_PRCS_ID,
exp_SrcFields.in_LOCATR_SBTYPE_CD as in_LOCATR_SBTYPE_CD,
exp_SrcFields.in_ADDR_SBTYPE_CD as in_ADDR_SBTYPE_CD,
LKP_STREETADDR.STREET_ADDR_ID as lkp_STREET_ADDR_ID_UPD,
LKP_STREETADDR.ADDR_LN_1_TXT as lkp_ADDR_LN_1_TXT,
LKP_STREETADDR.ADDR_LN_2_TXT as lkp_ADDR_LN_2_TXT,
LKP_STREETADDR.ADDR_LN_3_TXT as lkp_ADDR_LN_3_TXT,
LKP_STREETADDR.DWLNG_TYPE_CD as lkp_DWLNG_TYPE_CD,
LKP_STREETADDR.CITY_ID as lkp_CITY_ID,
LKP_STREETADDR.TERR_ID as lkp_TERR_ID,
LKP_STREETADDR.POSTL_CD_ID as lkp_POSTL_CD_ID,
LKP_STREETADDR.CTRY_ID as lkp_CTRY_ID,
LKP_STREETADDR.CNTY_ID as lkp_CNTY_ID,
LKP_STREETADDR.SPTL_PNT as lkp_SPTL_PNT,
LKP_STREETADDR.LOCTR_SBTYPE_CD as lkp_LOCTR_SBTYPE_CD,
LKP_STREETADDR.ADDR_SBTYPE_CD as lkp_ADDR_SBTYPE_CD,
LKP_STREETADDR.PRCS_ID as lkp_PRCS_ID,
LKP_STREETADDR.GEOCODE_STS_TYPE_CD as lkp_GEOCODE_STS_TYPE_CD,
LKP_STREETADDR.ADDR_STDZN_TYPE_CD as lkp_ADDR_STDZN_TYPE_CD,
LKP_STREETADDR.EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_SrcFields.out_Geocode_Status_Cd as out_Geocode_Status_Cd,
exp_SrcFields.out_Address_Standard_cd as out_Address_Standard_cd,
exp_SrcFields.out_TAX_LOC_ID as out_TAX_LOC_ID,
LKP_STREETADDR.TAX_LOC_ID as LKP_TAX_LOC_ID,
LKP_STREETADDR.LAT_MEAS as lkp_LAT_MEAS,
LKP_STREETADDR.LNGTD_MEAS as lkp_LNGTD_MEAS,
CASE WHEN ltrim ( rtrim ( exp_SrcFields.out_SPL_PNT ) ) IS NULL and ltrim ( rtrim ( LKP_STREETADDR.SPTL_PNT ) ) != NULL THEN ltrim ( rtrim ( LKP_STREETADDR.LAT_MEAS ) ) ELSE ltrim ( rtrim ( exp_SrcFields.out_Latitude ) ) END as v_Latitude,
CASE WHEN ltrim ( rtrim ( exp_SrcFields.out_SPL_PNT ) ) IS NULL and ltrim ( rtrim ( LKP_STREETADDR.SPTL_PNT ) ) != NULL THEN ltrim ( rtrim ( LKP_STREETADDR.LNGTD_MEAS ) ) ELSE ltrim ( rtrim ( exp_SrcFields.out_Longitude ) ) END as v_Longitude,
v_Latitude as o_Latitude,
v_Longitude as o_Longitude,
MD5 ( ltrim ( rtrim ( exp_SrcFields.in_DWLNG_TYPE_CD ) ) || CASE WHEN ltrim ( rtrim ( exp_SrcFields.out_SPL_PNT ) ) IS NULL and ltrim ( rtrim ( LKP_STREETADDR.SPTL_PNT ) ) != NULL THEN ltrim ( rtrim ( LKP_STREETADDR.SPTL_PNT ) ) ELSE ltrim ( rtrim ( exp_SrcFields.out_SPL_PNT ) ) END || ltrim ( rtrim ( exp_SrcFields.in_LOCATR_SBTYPE_CD ) ) || ltrim ( rtrim ( exp_SrcFields.in_ADDR_SBTYPE_CD ) ) || ltrim ( rtrim ( exp_SrcFields.out_Geocode_Status_Cd ) ) || ltrim ( rtrim ( exp_SrcFields.out_Address_Standard_cd ) ) || to_char ( exp_SrcFields.out_TAX_LOC_ID ) || to_char ( v_Latitude ) || to_char ( v_Longitude ) ) as v_SRC_MD5,
MD5 ( ltrim ( rtrim ( LKP_STREETADDR.DWLNG_TYPE_CD ) ) || ltrim ( rtrim ( LKP_STREETADDR.SPTL_PNT ) ) || ltrim ( rtrim ( LKP_STREETADDR.LOCTR_SBTYPE_CD ) ) || ltrim ( rtrim ( LKP_STREETADDR.ADDR_SBTYPE_CD ) ) || ltrim ( rtrim ( LKP_STREETADDR.GEOCODE_STS_TYPE_CD ) ) || ltrim ( rtrim ( LKP_STREETADDR.ADDR_STDZN_TYPE_CD ) ) || to_char ( LKP_STREETADDR.TAX_LOC_ID ) || to_char ( LKP_STREETADDR.LAT_MEAS ) || to_char ( LKP_STREETADDR.LNGTD_MEAS ) ) as v_TGT_MD5,
CASE WHEN v_TGT_MD5 IS NULL THEN ''I'' ELSE CASE WHEN v_SRC_MD5 = v_TGT_MD5 THEN ''X'' ELSE ''U'' END END as o_SRC_TGT,
CURRENT_TIMESTAMP as StartTime,
TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as EndDate,
exp_SrcFields.Retired as Retired,
LKP_STREETADDR.EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_SrcFields.source_record_id
FROM
exp_SrcFields
INNER JOIN LKP_STREETADDR ON exp_SrcFields.source_record_id = LKP_STREETADDR.source_record_id
);


-- Component RTRTRANS_Insert, Type ROUTER Output Group Insert
CREATE OR REPLACE TEMPORARY TABLE RTRTRANS_Insert AS (
SELECT
exp_CDC_Check.in_ADDR_LN_1_TXT as in_ADDR_LN_1_TXT,
exp_CDC_Check.in_ADDR_LN_2_TXT as in_ADDR_LN_2_TXT,
exp_CDC_Check.in_ADDR_LN_3_TXT as in_ADDR_LN_3_TXT,
exp_CDC_Check.in_DWLNG_TYPE_CD as in_DWLNG_TYPE_CD,
exp_CDC_Check.in_CITY_ID as in_CITY_ID,
exp_CDC_Check.in_TERR_ID as in_TERR_ID,
exp_CDC_Check.in_POSTL_CD_ID as in_POSTL_CD_ID,
exp_CDC_Check.in_CTRY_ID as in_CTRY_ID,
exp_CDC_Check.in_CNTY_ID as in_CNTY_ID,
exp_CDC_Check.out_SPL_PNT as in_SPL_PNT,
exp_CDC_Check.in_PRCS_ID as in_PRCS_ID,
exp_CDC_Check.in_LOCATR_SBTYPE_CD as in_LOCATR_SBTYPE_CD,
exp_CDC_Check.in_ADDR_SBTYPE_CD as in_ADDR_SBTYPE_CD,
exp_CDC_Check.lkp_STREET_ADDR_ID_UPD as lkp_STREET_ADDR_ID_UPD,
exp_CDC_Check.lkp_ADDR_LN_1_TXT as lkp_ADDR_LN_1_TXT,
exp_CDC_Check.lkp_ADDR_LN_2_TXT as lkp_ADDR_LN_2_TXT,
exp_CDC_Check.lkp_ADDR_LN_3_TXT as lkp_ADDR_LN_3_TXT,
exp_CDC_Check.lkp_CITY_ID as lkp_CITY_ID,
exp_CDC_Check.lkp_TERR_ID as lkp_TERR_ID,
exp_CDC_Check.lkp_POSTL_CD_ID as lkp_POSTL_CD_ID,
exp_CDC_Check.lkp_CTRY_ID as lkp_CTRY_ID,
exp_CDC_Check.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_CDC_Check.o_SRC_TGT as o_SRC_TGT,
exp_CDC_Check.StartTime as StartTime,
exp_CDC_Check.EndDate as EndDate,
exp_CDC_Check.Retired as Retired,
exp_CDC_Check.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_CDC_Check.out_Geocode_Status_Cd as out_Geocode_Status_Cd,
exp_CDC_Check.out_Address_Standard_cd as out_Address_Standard_cd,
exp_CDC_Check.out_TAX_LOC_ID as out_TAX_LOC_ID,
exp_CDC_Check.lkp_PRCS_ID as lkp_PRCS_ID,
exp_CDC_Check.lkp_DWLNG_TYPE_CD as lkp_DWLNG_TYPE_CD,
exp_CDC_Check.lkp_CNTY_ID as lkp_CNTY_ID,
exp_CDC_Check.lkp_SPTL_PNT as lkp_SPTL_PNT,
exp_CDC_Check.lkp_LOCTR_SBTYPE_CD as lkp_LOCTR_SBTYPE_CD,
exp_CDC_Check.lkp_ADDR_SBTYPE_CD as lkp_ADDR_SBTYPE_CD,
exp_CDC_Check.lkp_GEOCODE_STS_TYPE_CD as lkp_GEOCODE_STS_TYPE_CD,
exp_CDC_Check.lkp_ADDR_STDZN_TYPE_CD as lkp_ADDR_STDZN_TYPE_CD,
exp_CDC_Check.LKP_TAX_LOC_ID as LKP_TAX_LOC_ID,
exp_CDC_Check.lkp_LAT_MEAS as lkp_LAT_MEAS,
exp_CDC_Check.lkp_LNGTD_MEAS as lkp_LNGTD_MEAS,
exp_CDC_Check.o_Latitude as out_Latitude,
exp_CDC_Check.o_Longitude as out_Longitude,
exp_CDC_Check.source_record_id
FROM
exp_CDC_Check
WHERE ( exp_CDC_Check.o_SRC_TGT = ''I'' ) -- and exp_CDC_Check.lkp_STREET_ADDR_ID_UPD IS NULL ) OR ( ( exp_CDC_Check.o_SRC_TGT = ''U'' and exp_CDC_Check.lkp_STREET_ADDR_ID_UPD IS NOT NULL ) AND exp_CDC_Check.lkp_EDW_END_DTTM = TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
);


-- Component RTRTRANS_Update, Type ROUTER Output Group Update
CREATE OR REPLACE TEMPORARY TABLE RTRTRANS_Update AS (
SELECT
exp_CDC_Check.in_ADDR_LN_1_TXT as in_ADDR_LN_1_TXT,
exp_CDC_Check.in_ADDR_LN_2_TXT as in_ADDR_LN_2_TXT,
exp_CDC_Check.in_ADDR_LN_3_TXT as in_ADDR_LN_3_TXT,
exp_CDC_Check.in_DWLNG_TYPE_CD as in_DWLNG_TYPE_CD,
exp_CDC_Check.in_CITY_ID as in_CITY_ID,
exp_CDC_Check.in_TERR_ID as in_TERR_ID,
exp_CDC_Check.in_POSTL_CD_ID as in_POSTL_CD_ID,
exp_CDC_Check.in_CTRY_ID as in_CTRY_ID,
exp_CDC_Check.in_CNTY_ID as in_CNTY_ID,
exp_CDC_Check.out_SPL_PNT as in_SPL_PNT,
exp_CDC_Check.in_PRCS_ID as in_PRCS_ID,
exp_CDC_Check.in_LOCATR_SBTYPE_CD as in_LOCATR_SBTYPE_CD,
exp_CDC_Check.in_ADDR_SBTYPE_CD as in_ADDR_SBTYPE_CD,
exp_CDC_Check.lkp_STREET_ADDR_ID_UPD as lkp_STREET_ADDR_ID_UPD,
exp_CDC_Check.lkp_ADDR_LN_1_TXT as lkp_ADDR_LN_1_TXT,
exp_CDC_Check.lkp_ADDR_LN_2_TXT as lkp_ADDR_LN_2_TXT,
exp_CDC_Check.lkp_ADDR_LN_3_TXT as lkp_ADDR_LN_3_TXT,
exp_CDC_Check.lkp_CITY_ID as lkp_CITY_ID,
exp_CDC_Check.lkp_TERR_ID as lkp_TERR_ID,
exp_CDC_Check.lkp_POSTL_CD_ID as lkp_POSTL_CD_ID,
exp_CDC_Check.lkp_CTRY_ID as lkp_CTRY_ID,
exp_CDC_Check.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_CDC_Check.o_SRC_TGT as o_SRC_TGT,
exp_CDC_Check.StartTime as StartTime,
exp_CDC_Check.EndDate as EndDate,
exp_CDC_Check.Retired as Retired,
exp_CDC_Check.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_CDC_Check.out_Geocode_Status_Cd as out_Geocode_Status_Cd,
exp_CDC_Check.out_Address_Standard_cd as out_Address_Standard_cd,
exp_CDC_Check.out_TAX_LOC_ID as out_TAX_LOC_ID,
exp_CDC_Check.lkp_PRCS_ID as lkp_PRCS_ID,
exp_CDC_Check.lkp_DWLNG_TYPE_CD as lkp_DWLNG_TYPE_CD,
exp_CDC_Check.lkp_CNTY_ID as lkp_CNTY_ID,
exp_CDC_Check.lkp_SPTL_PNT as lkp_SPTL_PNT,
exp_CDC_Check.lkp_LOCTR_SBTYPE_CD as lkp_LOCTR_SBTYPE_CD,
exp_CDC_Check.lkp_ADDR_SBTYPE_CD as lkp_ADDR_SBTYPE_CD,
exp_CDC_Check.lkp_GEOCODE_STS_TYPE_CD as lkp_GEOCODE_STS_TYPE_CD,
exp_CDC_Check.lkp_ADDR_STDZN_TYPE_CD as lkp_ADDR_STDZN_TYPE_CD,
exp_CDC_Check.LKP_TAX_LOC_ID as LKP_TAX_LOC_ID,
exp_CDC_Check.lkp_LAT_MEAS as lkp_LAT_MEAS,
exp_CDC_Check.lkp_LNGTD_MEAS as lkp_LNGTD_MEAS,
exp_CDC_Check.o_Latitude as out_Latitude,
exp_CDC_Check.o_Longitude as out_Longitude,
exp_CDC_Check.source_record_id
FROM
exp_CDC_Check
WHERE exp_CDC_Check.o_SRC_TGT = ''U''
);


-- Component exp_pass_to_target_insert, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_target_insert AS
(
SELECT
SEQ_m_base_street_addr_insupd.NEXTVAL as var_STREET_ADDR_ID,
var_STREET_ADDR_ID as in_STREET_ADDR_ID,
RTRTRANS_Insert.in_ADDR_LN_1_TXT as in_ADDR_LN_1_TXT1,
RTRTRANS_Insert.in_ADDR_LN_2_TXT as in_ADDR_LN_2_TXT1,
RTRTRANS_Insert.in_ADDR_LN_3_TXT as in_ADDR_LN_3_TXT1,
RTRTRANS_Insert.in_DWLNG_TYPE_CD as in_DWLNG_TYPE_CD1,
RTRTRANS_Insert.in_CITY_ID as in_CITY_ID1,
RTRTRANS_Insert.in_TERR_ID as in_TERR_ID1,
RTRTRANS_Insert.in_POSTL_CD_ID as in_POSTL_CD_ID1,
RTRTRANS_Insert.in_CTRY_ID as in_CTRY_ID1,
RTRTRANS_Insert.in_CNTY_ID as in_CNTY_ID1,
RTRTRANS_Insert.in_SPL_PNT as in_SPL_PNT1,
RTRTRANS_Insert.in_LOCATR_SBTYPE_CD as in_LOCATR_SBTYPE_CD1,
RTRTRANS_Insert.in_ADDR_SBTYPE_CD as in_ADDR_SBTYPE_CD1,
RTRTRANS_Insert.in_PRCS_ID as in_PRCS_ID1,
RTRTRANS_Insert.StartTime as StartTime2,
RTRTRANS_Insert.EndDate as EDW_END_DTTM,
RTRTRANS_Insert.out_Geocode_Status_Cd as out_Geocode_Status_Cd1,
RTRTRANS_Insert.out_Address_Standard_cd as out_Address_Standard_cd1,
RTRTRANS_Insert.out_TAX_LOC_ID as out_TAX_LOC_ID1,
RTRTRANS_Insert.out_Latitude as out_Latitude1,
RTRTRANS_Insert.out_Longitude as out_Longitude1,
RTRTRANS_Insert.source_record_id
FROM
RTRTRANS_Insert
);


-- Component exp_pass_to_target_update, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_target_update AS
(
SELECT
RTRTRANS_Update.lkp_STREET_ADDR_ID_UPD as lkp_STREET_ADDR_ID_UPD3,
RTRTRANS_Update.in_ADDR_LN_1_TXT as ADDR_LN_1_TXT,
RTRTRANS_Update.in_ADDR_LN_2_TXT as ADDR_LN_2_TXT,
RTRTRANS_Update.in_ADDR_LN_3_TXT as ADDR_LN_3_TXT,
RTRTRANS_Update.in_DWLNG_TYPE_CD as DWLNG_TYPE_CD,
RTRTRANS_Update.in_CITY_ID as CITY_ID,
RTRTRANS_Update.in_TERR_ID as TERR_ID,
RTRTRANS_Update.in_POSTL_CD_ID as POSTL_CD_ID,
RTRTRANS_Update.in_CTRY_ID as CTRY_ID,
RTRTRANS_Update.in_CNTY_ID as CNTY_ID,
RTRTRANS_Update.in_SPL_PNT as SPTL_PNT,
RTRTRANS_Update.in_LOCATR_SBTYPE_CD as LOCTR_SBTYPE_CD,
RTRTRANS_Update.in_ADDR_SBTYPE_CD as ADDR_SBTYPE_CD,
RTRTRANS_Update.out_Geocode_Status_Cd as GEOCODE_STS_TYPE_CD,
RTRTRANS_Update.out_Address_Standard_cd as ADDR_STDZN_TYPE_CD,
RTRTRANS_Update.out_TAX_LOC_ID as TAX_LOC_ID,
RTRTRANS_Update.in_PRCS_ID as PRCS_ID,
RTRTRANS_Update.StartTime as StartTime3,
RTRTRANS_Update.EndDate as out_EDW_END_DTTM,
RTRTRANS_Update.out_Latitude as out_Latitude3,
RTRTRANS_Update.out_Longitude as out_Longitude3,
RTRTRANS_Update.source_record_id
FROM
RTRTRANS_Update
);


-- Component un_merge_ins_upd, Type UNION_TRANSFORMATION 
CREATE OR REPLACE TEMPORARY TABLE un_merge_ins_upd AS
(
/* Union Group Insert */
SELECT
exp_pass_to_target_insert.in_STREET_ADDR_ID,
exp_pass_to_target_insert.in_ADDR_LN_1_TXT1,
exp_pass_to_target_insert.in_ADDR_LN_2_TXT1,
exp_pass_to_target_insert.in_ADDR_LN_3_TXT1,
exp_pass_to_target_insert.in_DWLNG_TYPE_CD1,
exp_pass_to_target_insert.in_CITY_ID1,
exp_pass_to_target_insert.in_TERR_ID1,
exp_pass_to_target_insert.in_POSTL_CD_ID1,
exp_pass_to_target_insert.in_CTRY_ID1,
exp_pass_to_target_insert.in_CNTY_ID1,
exp_pass_to_target_insert.in_SPL_PNT1,
exp_pass_to_target_insert.in_LOCATR_SBTYPE_CD1,
exp_pass_to_target_insert.in_ADDR_SBTYPE_CD1,
exp_pass_to_target_insert.out_Geocode_Status_Cd1,
exp_pass_to_target_insert.out_Address_Standard_cd1,
exp_pass_to_target_insert.out_TAX_LOC_ID1,
exp_pass_to_target_insert.in_PRCS_ID1,
exp_pass_to_target_insert.StartTime2,
exp_pass_to_target_insert.EDW_END_DTTM,
exp_pass_to_target_insert.out_Latitude1 as Latitude,
exp_pass_to_target_insert.out_Longitude1 as Longitude,
exp_pass_to_target_insert.source_record_id
FROM exp_pass_to_target_insert
UNION ALL
/* Union Group Update */
SELECT
exp_pass_to_target_update.lkp_STREET_ADDR_ID_UPD3 as in_STREET_ADDR_ID,
exp_pass_to_target_update.ADDR_LN_1_TXT as in_ADDR_LN_1_TXT1,
exp_pass_to_target_update.ADDR_LN_2_TXT as in_ADDR_LN_2_TXT1,
exp_pass_to_target_update.ADDR_LN_3_TXT as in_ADDR_LN_3_TXT1,
exp_pass_to_target_update.DWLNG_TYPE_CD as in_DWLNG_TYPE_CD1,
exp_pass_to_target_update.CITY_ID as in_CITY_ID1,
exp_pass_to_target_update.TERR_ID as in_TERR_ID1,
exp_pass_to_target_update.POSTL_CD_ID as in_POSTL_CD_ID1,
exp_pass_to_target_update.CTRY_ID as in_CTRY_ID1,
exp_pass_to_target_update.CNTY_ID as in_CNTY_ID1,
exp_pass_to_target_update.SPTL_PNT as in_SPL_PNT1,
exp_pass_to_target_update.LOCTR_SBTYPE_CD as in_LOCATR_SBTYPE_CD1,
exp_pass_to_target_update.ADDR_SBTYPE_CD as in_ADDR_SBTYPE_CD1,
exp_pass_to_target_update.GEOCODE_STS_TYPE_CD as out_Geocode_Status_Cd1,
exp_pass_to_target_update.ADDR_STDZN_TYPE_CD as out_Address_Standard_cd1,
exp_pass_to_target_update.TAX_LOC_ID as out_TAX_LOC_ID1,
exp_pass_to_target_update.PRCS_ID as in_PRCS_ID1,
exp_pass_to_target_update.StartTime3 as StartTime2,
exp_pass_to_target_update.out_EDW_END_DTTM as EDW_END_DTTM,
exp_pass_to_target_update.out_Latitude3 as Latitude,
exp_pass_to_target_update.out_Longitude3 as Longitude,
exp_pass_to_target_update.source_record_id
FROM exp_pass_to_target_update
);


-- Component tgt_street_addr_Insert_Update, Type TARGET 
INSERT INTO DB_T_PROD_CORE.STREET_ADDR
(
STREET_ADDR_ID,
ADDR_LN_1_TXT,
ADDR_LN_2_TXT,
ADDR_LN_3_TXT,
DWLNG_TYPE_CD,
CITY_ID,
TERR_ID,
POSTL_CD_ID,
CTRY_ID,
CNTY_ID,
SPTL_PNT,
LAT_MEAS,
LNGTD_MEAS,
LOCTR_SBTYPE_CD,
ADDR_SBTYPE_CD,
GEOCODE_STS_TYPE_CD,
ADDR_STDZN_TYPE_CD,
TAX_LOC_ID,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM
)
SELECT
un_merge_ins_upd.in_STREET_ADDR_ID as STREET_ADDR_ID,
un_merge_ins_upd.in_ADDR_LN_1_TXT1 as ADDR_LN_1_TXT,
un_merge_ins_upd.in_ADDR_LN_2_TXT1 as ADDR_LN_2_TXT,
un_merge_ins_upd.in_ADDR_LN_3_TXT1 as ADDR_LN_3_TXT,
un_merge_ins_upd.in_DWLNG_TYPE_CD1 as DWLNG_TYPE_CD,
un_merge_ins_upd.in_CITY_ID1 as CITY_ID,
un_merge_ins_upd.in_TERR_ID1 as TERR_ID,
un_merge_ins_upd.in_POSTL_CD_ID1 as POSTL_CD_ID,
un_merge_ins_upd.in_CTRY_ID1 as CTRY_ID,
un_merge_ins_upd.in_CNTY_ID1 as CNTY_ID,
un_merge_ins_upd.in_SPL_PNT1 as SPTL_PNT,
un_merge_ins_upd.Latitude as LAT_MEAS,
un_merge_ins_upd.Longitude as LNGTD_MEAS,
un_merge_ins_upd.in_LOCATR_SBTYPE_CD1 as LOCTR_SBTYPE_CD,
un_merge_ins_upd.in_ADDR_SBTYPE_CD1 as ADDR_SBTYPE_CD,
un_merge_ins_upd.out_Geocode_Status_Cd1 as GEOCODE_STS_TYPE_CD,
un_merge_ins_upd.out_Address_Standard_cd1 as ADDR_STDZN_TYPE_CD,
un_merge_ins_upd.out_TAX_LOC_ID1 as TAX_LOC_ID,
un_merge_ins_upd.in_PRCS_ID1 as PRCS_ID,
un_merge_ins_upd.StartTime2 as EDW_STRT_DTTM,
un_merge_ins_upd.EDW_END_DTTM as EDW_END_DTTM
FROM
un_merge_ins_upd;


-- Component tgt_street_addr_Insert_Update, Type Post SQL 
UPDATE db_t_prod_core.STREET_ADDR 
SET EDW_END_DTTM = A.lead1
FROM

(

SELECT	distinct STREET_ADDR_ID,EDW_STRT_DTTM,

max(EDW_STRT_DTTM) over (partition by STREET_ADDR_ID ORDER	BY EDW_STRT_DTTM ASC rows between 1 following 

and	1 following) - INTERVAL ''1 SECOND''  as lead1

FROM	db_t_prod_core.STREET_ADDR) A



WHERE	

STREET_ADDR.EDW_STRT_DTTM = A.EDW_STRT_DTTM 

AND	STREET_ADDR.STREET_ADDR_ID = A.STREET_ADDR_ID

AND CAST(STREET_ADDR.EDW_END_DTTM AS DATE)=''9999-12-31''

AND	A.lead1 is not null;


END; 
';