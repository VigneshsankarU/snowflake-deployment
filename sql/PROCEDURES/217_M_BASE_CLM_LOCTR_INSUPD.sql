-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_CLM_LOCTR_INSUPD("RUN_ID" VARCHAR)
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

-- Component LKP_TERADATA_ETL_REF_XLAT_CLM_LOCTR_ROLE_CD, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_CLM_LOCTR_ROLE_CD AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''CLM_LOCTR_ROLE''

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived''

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS= ''DS''

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_CLM_SRC_CD, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_CLM_SRC_CD AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''SRC_SYS''

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived''

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS= ''DS''

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component sq_claim_associate_stag_x, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_claim_associate_stag_x AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as claimnumber,
$2 as locatorrolecode,
$3 as locatortype,
$4 as county,
$5 as City,
$6 as Zipcode,
$7 as Addline2,
$8 as Addline1,
$9 as CountryTypeCode,
$10 as StateTypeCode,
$11 as Retired,
$12 as CLM_SRC_CD,
$13 as updatetime,
$14 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT Claim_Associate_Stag_x.claimnumber , Claim_Associate_Stag_x.locatorrolecode,  locatortype,  Claim_Associate_Stag_x.county, city, Claim_Associate_Stag_x.Zipcode, Claim_Associate_Stag_x.Adline2, Claim_Associate_Stag_x.Adline1, Claim_Associate_Stag_x.CountryTypeCode, Claim_Associate_Stag_x.StateTypeCode, 

Claim_Associate_Stag_x.Retired, 

''SRC_SYS6'' as clm_src_cd ,Claim_Associate_Stag_x.updatetime

FROM 

(

select distinct cc_claim.ClaimNumber, cc_claim.locatorrolecode, cc_claim.locatortype, cc_claim.countrytypecode, cc_claim.statetypecode, cc_claim.county,city, 

cc_claim.zipcode, cc_claim.adline1, cc_claim.adline2, cc_claim.UpdateTime, cc_claim.Retired

from 

(select cast(jur.claimnumber as VARCHAR(40)) as claimnumber,  cast(jur.locatorrolecode as VARCHAR(500)) as locatorrolecode, cast(jur.locatortype  as VARCHAR(500))as locatortype, cast(jur.countrytypecode as VARCHAR(100)) as countrytypecode, cast(jur.statetypecode as VARCHAR(100)) as statetypecode, cast(jur.county as VARCHAR(500)) as county, cast(null as VARCHAR(500)) as city, 

cast(null as VARCHAR(500)) as zipcode, 

cast(null as VARCHAR(1000)) as adline1, 

cast(null as VARCHAR(1000)) as adline2,

cast(jur.updatetime as TIMESTAMP(6)) as updatetime, 

cast(jur.Retired as BIGINT) as Retired

from

(Select distinct cast(cc_claim.ClaimNumber_stg as VARCHAR(40)) as ClaimNumber, cast(''JURLOC'' as VARCHAR(500)) as locatorrolecode,cast(''STATE'' as VARCHAR(500)) as locatortype, cast(cctl_country.TYPECODE_stg as VARCHAR(100)) as countrytypecode, cast(cctl_state.TYPECODE_stg as VARCHAR(100)) as statetypecode,

cast(cc_address.County_stg as VARCHAR(500)) as county, cast(cc_address.UpdateTime_stg as TIMESTAMP(6)) as UpdateTime,

cast(case when cc_claim.retired_stg=0 and cctl_jurisdiction.retired_stg=0 and cc_policy.retired_stg=0 and cc_policylocation.retired_stg=0 and cc_address.retired_stg=0 and cctl_country.retired_stg=0 and cctl_state.retired_stg=0 then 0 else 1 end as BIGINT) as Retired

from (select cc_claim.ClaimNumber_stg, cc_claim.id_stg, cc_claim.retired_stg, cc_claim.JurisdictionState_stg, PolicyID_stg from DB_T_PROD_STAG.cc_claim inner join DB_T_PROD_STAG.cctl_claimstate on cc_claim.State_stg= cctl_claimstate.id_stg where cctl_claimstate.name_stg <> ''Draft'') cc_claim 

join DB_T_PROD_STAG.cctl_jurisdiction on cc_claim.JurisdictionState_stg=cctl_jurisdiction.id_stg

join DB_T_PROD_STAG.cc_policy on cc_claim.PolicyID_stg= cc_policy.id_stg

join DB_T_PROD_STAG.cc_policylocation  on cc_policy.id_stg = cc_policylocation.PolicyID_stg

join DB_T_PROD_STAG.cc_address on cc_policylocation.AddressID_stg = cc_address.Id_stg

join DB_T_PROD_STAG.cctl_country on cc_address.Country_stg = cctl_country.id_stg

join DB_T_PROD_STAG.cctl_state on cc_address.State_stg = cctl_state.id_stg



/*UNION ALL
Select distinct cast(cc_claim.ClaimNumber_stg as VARCHAR(40)) as ClaimNumber, cast(''JURLOC'' as VARCHAR(500)) as locatorrolecode,  cast(''COUNTY'' as VARCHAR(500)) as locatortype, cast(cctl_country.TYPECODE_stg as VARCHAR(100)) as countrytypecode, cast(cctl_state.TYPECODE_stg as VARCHAR(100)) as statetypecode, 

cast(cc_address.County_stg as VARCHAR(500)) as county, cc_claim.UpdateTime_stg as UpdateTime, 

case when cc_claim.retired_stg=0 and cctl_jurisdiction.retired_stg=0 and cc_policy.retired_stg=0 and cc_policylocation.retired_stg=0 and cc_address.retired_stg=0 and cctl_country.retired_stg=0 and cctl_state.retired_stg=0 then 0 else 1 end as Retired

from (select cc_claim.ClaimNumber_stg, cc_claim.id_stg, cc_claim.retired_stg, cc_claim.JurisdictionState_stg, PolicyID_stg from DB_T_PROD_STAG.cc_claim inner join DB_T_PROD_STAG.cctl_claimstate on cc_claim.State_stg= cctl_claimstate.id_stg where cctl_claimstate.name_stg <> ''Draft'') DB_T_PROD_STAG.cc_claim 

join DB_T_PROD_STAG.cctl_jurisdiction on cc_claim.JurisdictionState_stg=cctl_jurisdiction.id_stg

join DB_T_PROD_STAG.cc_policy on cc_claim.PolicyID_stg= cc_policy.id_stg

join DB_T_PROD_STAG.cc_policylocation  on cc_policy.id_stg = cc_policylocation.PolicyID_stg

join DB_T_PROD_STAG.cc_address on cc_policylocation.AddressID_stg = cc_address.Id_stg

join DB_T_PROD_STAG.cctl_country on cc_address.Country_stg = cctl_country.id_stg

join DB_T_PROD_STAG.cctl_state on DB_T_SHRD_PROD.State = cctl_state.id_stg*/

)  jur



UNION ALL



select loss.ClaimNumber, loss.locatorrolecode, loss.locatortype, loss.countrytypecode, loss.statetypecode, loss.county, city, loss.zipcode, loss.adline1, loss.adline2,

loss.UpdateTime,

loss.Retired

from

(select distinct cast(cc_claim.ClaimNumber_stg as VARCHAR(40)) as ClaimNumber,cast(''LOSSLOC'' as VARCHAR(500)) as locatorrolecode, cast(''STATE'' as VARCHAR(500)) as locatortype, cast(cctl_country.TYPECODE_stg as VARCHAR(100)) as countrytypecode,

cast(cctl_state.TYPECODE_stg as VARCHAR(100)) as statetypecode, cast(cc_address.County_stg as VARCHAR(500)) as county, cast(null as VARCHAR(500)) as city, cast(null as VARCHAR(500)) as zipcode, cast(null as VARCHAR(1000)) as adline1, cast(null as VARCHAR(1000)) as adline2 , cast(cc_address.UpdateTime_stg  as TIMESTAMP(6)) as UpdateTime, 

cast(case when cc_claim.retired_stg=0 and cc_address.retired_stg=0 and cctl_country.retired_stg=0 and cctl_state.retired_stg=0 then 0 else 1 end as BIGINT) as Retired

from (select cc_claim.ClaimNumber_stg, cc_claim.id_stg, cc_claim.retired_stg, cc_claim.LossLocationID_stg from DB_T_PROD_STAG.cc_claim inner join DB_T_PROD_STAG.cctl_claimstate on cc_claim.State_stg= cctl_claimstate.id_stg where cctl_claimstate.name_stg <> ''Draft'') cc_claim 

join DB_T_PROD_STAG.cc_address  on cc_claim.LossLocationID_stg = cc_address.id_stg

left outer join DB_T_PROD_STAG.cctl_country  on cc_address.Country_stg = cctl_country.id_stg

left outer join DB_T_PROD_STAG.cctl_state  on cc_address.State_stg = cctl_state.ID_stg



union all



select distinct cast(cc_claim.ClaimNumber_stg as VARCHAR(40)) as ClaimNumber,cast(''LOSSLOC'' as VARCHAR(500)) as locatorrolecode, cast(''COUNTY'' as VARCHAR(500)) as locatortype, cast(cctl_country.TYPECODE_stg as VARCHAR(100)) as countrytypecode,

cast(cctl_state.TYPECODE_stg as VARCHAR(100)) as statetypecode, cast(cc_address.County_stg as VARCHAR(500)) as county, cast(null as VARCHAR(500)) as city, cast(null as VARCHAR(500)) as zipcode, cast(null as VARCHAR(1000)) as adline1, cast(null as VARCHAR(1000)) as adline2,

cast(cc_address.UpdateTime_stg  as TIMESTAMP(6)) as UpdateTime, 

cast(case when cc_claim.retired_stg=0 and cc_address.retired_stg=0 and cctl_country.retired_stg=0 and cctl_state.retired_stg=0 then 0 else 1 end as BIGINT) as Retired

from (select cc_claim.ClaimNumber_stg, cc_claim.id_stg, cc_claim.retired_stg, cc_claim.LossLocationID_stg, PolicyID_stg from DB_T_PROD_STAG.cc_claim inner join DB_T_PROD_STAG.cctl_claimstate on cc_claim.State_stg= cctl_claimstate.id_stg where cctl_claimstate.name_stg <> ''Draft'') cc_claim 

join DB_T_PROD_STAG.cc_address  on cc_claim.LossLocationID_stg = cc_address.id_stg

left outer join DB_T_PROD_STAG.cctl_country  on cc_address.Country_stg = cctl_country.id_stg

left outer join DB_T_PROD_STAG.cctl_state  on cc_address.State_stg = cctl_state.ID_stg





union all



select distinct cast(cc_claim.ClaimNumber_stg as VARCHAR(40)) as ClaimNumber,cast(''LOSSLOC'' as VARCHAR(500)) as locatorrolecode, cast(''CITY'' as VARCHAR(500)) as locatortype, cast(cctl_country.TYPECODE_stg as VARCHAR(100)) as countrytypecode,

cast(cctl_state.TYPECODE_stg as VARCHAR(100)) as statetypecode, cast(cc_address.County_stg as VARCHAR(500)) as county, cast(cc_address.City_stg as VARCHAR(500)) as city, cast(null as VARCHAR(500)) as zipcode, cast(null as VARCHAR(1000)) as adline1, cast(null as VARCHAR(1000)) as adline2 ,

cast(cc_address.UpdateTime_stg  as TIMESTAMP(6)) as UpdateTime, 

cast(case when cc_claim.retired_stg=0 and cc_address.retired_stg=0 and cctl_country.retired_stg=0 and cctl_state.retired_stg=0 then 0 else 1 end as BIGINT) as Retired

from (select cc_claim.ClaimNumber_stg, cc_claim.id_stg, cc_claim.retired_stg, cc_claim.LossLocationID_stg from DB_T_PROD_STAG.cc_claim inner join DB_T_PROD_STAG.cctl_claimstate on cc_claim.State_stg= cctl_claimstate.id_stg where cctl_claimstate.name_stg <> ''Draft'') cc_claim 

join DB_T_PROD_STAG.cc_address  on cc_claim.LossLocationID_stg = cc_address.id_stg

left outer join DB_T_PROD_STAG.cctl_country  on cc_address.Country_stg = cctl_country.id_stg

left outer join DB_T_PROD_STAG.cctl_state  on cc_address.State_stg = cctl_state.ID_stg



union all



select distinct cast(cc_claim.ClaimNumber_stg as VARCHAR(40)) as ClaimNumber,cast(''LOSSLOC'' as VARCHAR(500)) as locatorrolecode, cast(''ZIP'' as VARCHAR(500)) as locatortype, cast(cctl_country.TYPECODE_stg as VARCHAR(100)) as countrytypecode,

cast(null as VARCHAR(100)) as statetypecode, cast(cc_address.County_stg as VARCHAR(500)) as county, cast(null as VARCHAR(500)) as city, cast(cc_address.PostalCode_stg as VARCHAR(500)) as zipcode, cast(null as VARCHAR(1000)) as adline1, cast(null as VARCHAR(1000)) as adline2 ,

cast(cc_address.UpdateTime_stg  as TIMESTAMP(6)) as UpdateTime, 

cast(case when cc_claim.retired_stg=0 and cc_address.retired_stg=0 and cctl_country.retired_stg=0 and cctl_state.retired_stg=0 then 0 else 1 end as BIGINT) as Retired

from (select cc_claim.ClaimNumber_stg, cc_claim.id_stg, cc_claim.retired_stg, LossLocationID_stg from DB_T_PROD_STAG.cc_claim inner join DB_T_PROD_STAG.cctl_claimstate on cc_claim.State_stg= cctl_claimstate.id_stg where cctl_claimstate.name_stg <> ''Draft'') cc_claim 

join DB_T_PROD_STAG.cc_address  on cc_claim.LossLocationID_stg = cc_address.id_stg

left outer join DB_T_PROD_STAG.cctl_country  on cc_address.Country_stg = cctl_country.id_stg

left outer join DB_T_PROD_STAG.cctl_state  on cc_address.State_stg = cctl_state.ID_stg





union all



select distinct cast(cc_claim.ClaimNumber_stg as VARCHAR(40)) as ClaimNumber,cast(''LOSSLOC'' as VARCHAR(500)) as locatorrolecode, cast(''STREET'' as VARCHAR(500)) as locatortype, cast(cctl_country.TYPECODE_stg as VARCHAR(100)) as countrytypecode,

cast(cctl_state.TYPECODE_stg as VARCHAR(100)) as statetypecode, cast(cc_address.County_stg as VARCHAR(500)) as county, cast(cc_address.City_stg as VARCHAR(500)) as city, cast(cc_address.PostalCode_stg as VARCHAR(500)) as zipcode, 

cast(cc_address.AddressLine1_stg as VARCHAR(1000)) as adline1, cast(cc_address.AddressLine2_stg as VARCHAR(1000)) as adline2 ,

cast(cc_address.UpdateTime_stg  as TIMESTAMP(6)) as UpdateTime, 

cast(case when cc_claim.retired_stg=0 and cc_address.retired_stg=0 and cctl_country.retired_stg=0 and cctl_state.retired_stg=0 then 0 else 1 end as BIGINT) as Retired

from (select cc_claim.ClaimNumber_stg, cc_claim.id_stg, cc_claim.retired_stg, cc_claim.LossLocationID_stg from DB_T_PROD_STAG.cc_claim inner join DB_T_PROD_STAG.cctl_claimstate on cc_claim.State_stg= cctl_claimstate.id_stg where cctl_claimstate.name_stg <> ''Draft'') cc_claim 

join DB_T_PROD_STAG.cc_address  on cc_claim.LossLocationID_stg = cc_address.id_stg

left outer join DB_T_PROD_STAG.cctl_country  on cc_address.Country_stg = cctl_country.id_stg

left outer join DB_T_PROD_STAG.cctl_state  on cc_address.State_stg = cctl_state.ID_stg

) loss



UNION ALL



SELECT cast(cc_claim.ClaimNumber_stg as VARCHAR(40)) as ClaimNumber, cast(case cctl_mattertype.typecode_stg when   ''Mediation'' then ''MEDLOC'' when ''Arbitration'' then ''ARBLOC'' end as VARCHAR(500)) as locatorrolecode,

cast(''STREET'' as VARCHAR(500)) as locatortype,

cast(cctl_country.TYPECODE_stg as VARCHAR(100)) as countrytypecode, cast(cctl_state.TYPECODE_stg as VARCHAR(100)) as statetypecode, cast(cc_address.County_stg as VARCHAR(500)) as county, cast(cc_address.City_stg as VARCHAR(500)) as city , cast(cc_address.PostalCode_stg as VARCHAR(500)) as zipcode, 

cast(cc_address.AddressLine1_stg as VARCHAR(1000)) as adline1, cast(cc_address.AddressLine2_stg as VARCHAR(1000)) as adline2, cast(cc_address.UpdateTime_stg  as TIMESTAMP(6)) as UpdateTime,

cast(case when cc_claim.retired_stg=0 and cc_address.retired_stg=0 and cctl_country.retired_stg=0 and cctl_state.retired_stg=0 and cc_matter.retired_stg=0 and cc_claimcontact.retired_stg=0

and cc_contact.retired_stg=0 and cctl_venuetype.retired_stg=0 then 0 else 1 end as BIGINT) as Retired

FROM (select cc_claim.ClaimNumber_stg, cc_claim.id_stg, cc_claim.retired_stg from DB_T_PROD_STAG.cc_claim inner join DB_T_PROD_STAG.cctl_claimstate on cc_claim.State_stg= cctl_claimstate.id_stg where cctl_claimstate.name_stg <> ''Draft'')  cc_claim

 join DB_T_PROD_STAG.cc_matter on cc_claim.id_stg = cc_matter.claimid_stg

 join DB_T_PROD_STAG.cc_claimcontact on cc_claim.id_stg = cc_claimcontact.claimid_stg

 join DB_T_PROD_STAG.cc_contact on cc_claimcontact.contactid_stg = cc_contact.id_stg

 join DB_T_PROD_STAG.cctl_contact on (cc_contact.subtype_stg = cctl_contact.id_stg and cctl_contact.TYPECODE_stg = ''LegalVenue'')

 join DB_T_PROD_STAG.cc_address on cc_contact.PrimaryAddressID_stg =  cc_address.id_stg

 join DB_T_PROD_STAG.cctl_venuetype on cc_contact.venuetype_stg = cctl_venuetype.id_stg

 join DB_T_PROD_STAG.cctl_mattertype on (cc_matter.mattertype_stg = cctl_mattertype.id_stg and cctl_mattertype.TYPECODE_stg in( ''Mediation'', ''Arbitration''))

 left outer join DB_T_PROD_STAG.cctl_country  on cc_address.Country_stg = cctl_country.id_stg

 left outer join DB_T_PROD_STAG.cctl_state  on cc_address.State_stg = cctl_state.ID_stg) cc_claim

 where cc_claim.UpdateTime >(:START_DTTM) AND cc_claim.UpdateTime <= (:END_DTTM)

)

Claim_Associate_Stag_x qualify row_number () over (partition by claimnumber,locatorrolecode,locatortype  order by updatetime desc)=1
) SRC
)
);


-- Component LKP_CTRY, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_CTRY AS
(
SELECT
LKP.CTRY_ID,
sq_claim_associate_stag_x.source_record_id,
ROW_NUMBER() OVER(PARTITION BY sq_claim_associate_stag_x.source_record_id ORDER BY LKP.CTRY_ID asc,LKP.CAL_TYPE_CD asc,LKP.ISO_3166_CTRY_NUM asc,LKP.GEOGRCL_AREA_SHRT_NAME asc,LKP.GEOGRCL_AREA_NAME asc,LKP.GEOGRCL_AREA_DESC asc,LKP.CURY_CD asc,LKP.GEOGRCL_AREA_STRT_DTTM asc,LKP.GEOGRCL_AREA_END_DTTM asc,LKP.PRCS_ID asc) RNK
FROM
sq_claim_associate_stag_x
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
) LKP ON LKP.GEOGRCL_AREA_SHRT_NAME = sq_claim_associate_stag_x.CountryTypeCode
QUALIFY RNK = 1
);


-- Component LKP_TERR, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERR AS
(
SELECT
LKP.TERR_ID,
LKP.CTRY_ID,
sq_claim_associate_stag_x.source_record_id,
ROW_NUMBER() OVER(PARTITION BY sq_claim_associate_stag_x.source_record_id ORDER BY LKP.TERR_ID asc,LKP.TERR_TYPE_CD asc,LKP.CTRY_ID asc,LKP.RGN_ID asc,LKP.GEOGRCL_AREA_SHRT_NAME asc,LKP.GEOGRCL_AREA_NAME asc,LKP.GEOGRCL_AREA_DESC asc,LKP.CURY_CD asc,LKP.GEOGRCL_AREA_STRT_DTTM asc,LKP.GEOGRCL_AREA_END_DTTM asc,LKP.PRCS_ID asc) RNK
FROM
sq_claim_associate_stag_x
INNER JOIN LKP_CTRY ON sq_claim_associate_stag_x.source_record_id = LKP_CTRY.source_record_id
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
) LKP ON LKP.CTRY_ID = LKP_CTRY.CTRY_ID AND LKP.GEOGRCL_AREA_SHRT_NAME = sq_claim_associate_stag_x.StateTypeCode
QUALIFY RNK = 1
);


-- Component LKP_POSTL_CD, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_POSTL_CD AS
(
SELECT
LKP.POSTL_CD_ID,
LKP.CTRY_ID,
sq_claim_associate_stag_x.source_record_id,
ROW_NUMBER() OVER(PARTITION BY sq_claim_associate_stag_x.source_record_id ORDER BY LKP.POSTL_CD_ID asc,LKP.CNTY_ID asc,LKP.CTRY_ID asc,LKP.POSTL_CD_NUM asc,LKP.TM_ZN_CD asc,LKP.GEOGRCL_AREA_SHRT_NAME asc,LKP.GEOGRCL_AREA_NAME asc,LKP.GEOGRCL_AREA_DESC asc,LKP.CURY_CD asc,LKP.GEOGRCL_AREA_STRT_DTTM asc,LKP.GEOGRCL_AREA_END_DTTM asc,LKP.PRCS_ID asc) RNK
FROM
sq_claim_associate_stag_x
INNER JOIN LKP_CTRY ON sq_claim_associate_stag_x.source_record_id = LKP_CTRY.source_record_id
LEFT JOIN (
SELECT
POSTL_CD_ID,
CNTY_ID,
CTRY_ID,
POSTL_CD_NUM,
TM_ZN_CD,
GEOGRCL_AREA_SHRT_NAME,
GEOGRCL_AREA_NAME,
GEOGRCL_AREA_DESC,
CURY_CD,
GEOGRCL_AREA_STRT_DTTM,
GEOGRCL_AREA_END_DTTM,
PRCS_ID
FROM DB_T_PROD_CORE.POSTL_CD
) LKP ON LKP.CTRY_ID = LKP_CTRY.CTRY_ID AND LKP.POSTL_CD_NUM = sq_claim_associate_stag_x.Zipcode
QUALIFY RNK = 1
);


-- Component exp_all_sources, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_all_sources AS
(
SELECT
sq_claim_associate_stag_x.claimnumber as claimnumber,
sq_claim_associate_stag_x.locatorrolecode as locatorrolecode,
sq_claim_associate_stag_x.locatortype as locatortype,
sq_claim_associate_stag_x.county as county,
sq_claim_associate_stag_x.City as City,
sq_claim_associate_stag_x.Zipcode as Zipcode,
sq_claim_associate_stag_x.Addline2 as Addline2,
sq_claim_associate_stag_x.Addline1 as Addline1,
sq_claim_associate_stag_x.CountryTypeCode as CountryTypeCode,
sq_claim_associate_stag_x.StateTypeCode as StateTypeCode,
sq_claim_associate_stag_x.Retired as Retired,
:PRCS_ID as PRCS_ID,
LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_CLM_SRC_CD */ as out_CLM_SRC_CD,
CURRENT_TIMESTAMP as EDW_STRT_DTTM,
TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as EDW_END_DTTM,
sq_claim_associate_stag_x.updatetime as TRANS_STRT_DTTM,
sq_claim_associate_stag_x.source_record_id,
row_number() over (partition by sq_claim_associate_stag_x.source_record_id order by sq_claim_associate_stag_x.source_record_id) as RNK
FROM
sq_claim_associate_stag_x
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_CLM_SRC_CD LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = sq_claim_associate_stag_x.CLM_SRC_CD
QUALIFY RNK = 1
);


-- Component LKP_CNTY, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_CNTY AS
(
SELECT
LKP.CNTY_ID,
LKP.TERR_ID,
sq_claim_associate_stag_x.source_record_id,
ROW_NUMBER() OVER(PARTITION BY sq_claim_associate_stag_x.source_record_id ORDER BY LKP.CNTY_ID asc,LKP.TERR_ID asc,LKP.GEOGRCL_AREA_SHRT_NAME asc,LKP.GEOGRCL_AREA_NAME asc,LKP.GEOGRCL_AREA_DESC asc,LKP.CURY_CD asc,LKP.GEOGRCL_AREA_STRT_DTTM asc,LKP.GEOGRCL_AREA_END_DTTM asc,LKP.PRCS_ID asc) RNK
FROM
sq_claim_associate_stag_x
INNER JOIN LKP_TERR ON sq_claim_associate_stag_x.source_record_id = LKP_TERR.source_record_id
LEFT JOIN (
SELECT CNTY.CNTY_ID as CNTY_ID, CNTY.GEOGRCL_AREA_NAME as GEOGRCL_AREA_NAME, CNTY.GEOGRCL_AREA_DESC as GEOGRCL_AREA_DESC, CNTY.CURY_CD as CURY_CD, CNTY.GEOGRCL_AREA_STRT_DTTM as GEOGRCL_AREA_STRT_DTTM, CNTY.GEOGRCL_AREA_END_DTTM as GEOGRCL_AREA_END_DTTM, CNTY.PRCS_ID as PRCS_ID, CNTY.TERR_ID as TERR_ID, CNTY.GEOGRCL_AREA_SHRT_NAME as GEOGRCL_AREA_SHRT_NAME FROM DB_T_PROD_CORE.CNTY 
WHERE CNTY.GEOGRCL_AREA_SHRT_NAME IS NOT NULL
) LKP ON LKP.TERR_ID = LKP_TERR.TERR_ID AND LKP.GEOGRCL_AREA_SHRT_NAME = sq_claim_associate_stag_x.county
QUALIFY RNK = 1
);


-- Component LKP_CITY, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_CITY AS
(
SELECT
LKP.CITY_ID,
LKP.TERR_ID,
sq_claim_associate_stag_x.source_record_id,
ROW_NUMBER() OVER(PARTITION BY sq_claim_associate_stag_x.source_record_id ORDER BY LKP.CITY_ID asc,LKP.CITY_TYPE_CD asc,LKP.TERR_ID asc,LKP.GEOGRCL_AREA_SHRT_NAME asc,LKP.GEOGRCL_AREA_NAME asc,LKP.GEOGRCL_AREA_DESC asc,LKP.CURY_CD asc,LKP.GEOGRCL_AREA_STRT_DTTM asc,LKP.GEOGRCL_AREA_END_DTTM asc,LKP.PRCS_ID asc) RNK
FROM
sq_claim_associate_stag_x
INNER JOIN LKP_TERR ON sq_claim_associate_stag_x.source_record_id = LKP_TERR.source_record_id
LEFT JOIN (
SELECT CITY.CITY_ID as CITY_ID, CITY.CITY_TYPE_CD as CITY_TYPE_CD, CITY.GEOGRCL_AREA_NAME as GEOGRCL_AREA_NAME, CITY.GEOGRCL_AREA_DESC as GEOGRCL_AREA_DESC, CITY.CURY_CD as CURY_CD, CITY.GEOGRCL_AREA_STRT_DTTM as GEOGRCL_AREA_STRT_DTTM, CITY.GEOGRCL_AREA_END_DTTM as GEOGRCL_AREA_END_DTTM, CITY.PRCS_ID as PRCS_ID, CITY.TERR_ID as TERR_ID, CITY.GEOGRCL_AREA_SHRT_NAME as GEOGRCL_AREA_SHRT_NAME FROM DB_T_PROD_CORE.CITY 
where CITY.GEOGRCL_AREA_SHRT_NAME is not null
) LKP ON LKP.TERR_ID = LKP_TERR.TERR_ID AND LKP.GEOGRCL_AREA_SHRT_NAME = sq_claim_associate_stag_x.City
QUALIFY RNK = 1
);


-- Component LKP_CLM, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_CLM AS
(
SELECT
LKP.CLM_ID,
LKP.PRCS_ID,
LKP.EDW_STRT_DTTM,
LKP.EDW_END_DTTM,
LKP.TRANS_STRT_DTTM,
exp_all_sources.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_all_sources.source_record_id ORDER BY LKP.CLM_ID desc,LKP.CLM_TYPE_CD desc,LKP.CLM_MDIA_TYPE_CD desc,LKP.CLM_SUBMTL_TYPE_CD desc,LKP.ACDNT_TYPE_CD desc,LKP.CLM_CTGY_TYPE_CD desc,LKP.ADDL_INSRNC_PLN_IND desc,LKP.EMPLMT_RLTD_IND desc,LKP.ATTNY_INVLVMT_IND desc,LKP.CLM_NUM desc,LKP.CLM_PRIR_IND desc,LKP.PMT_MODE_CD desc,LKP.CLM_OBLGTN_TYPE_CD desc,LKP.SUBRGTN_ELGBL_CD desc,LKP.SUBRGTN_ELGBLY_RSN_CD desc,LKP.CURY_CD desc,LKP.INCDT_EV_ID desc,LKP.INSRD_AT_FAULT_IND desc,LKP.CVGE_IN_QUES_IND desc,LKP.EXTNT_OF_FIRE_DMG_TYPE_CD desc,LKP.VFYD_CLM_IND desc,LKP.PRCS_ID desc,LKP.CLM_STRT_DTTM desc,LKP.CLM_END_DTTM desc,LKP.EDW_STRT_DTTM desc,LKP.EDW_END_DTTM desc,LKP.SRC_SYS_CD desc,LKP.TRANS_STRT_DTTM desc,LKP.LGCY_CLM_NUM desc) RNK
FROM
exp_all_sources
LEFT JOIN (
SELECT CLM.CLM_ID as CLM_ID, CLM.CLM_TYPE_CD as CLM_TYPE_CD, CLM.CLM_MDIA_TYPE_CD as CLM_MDIA_TYPE_CD, CLM.CLM_SUBMTL_TYPE_CD as CLM_SUBMTL_TYPE_CD, CLM.ACDNT_TYPE_CD as ACDNT_TYPE_CD, CLM.CLM_CTGY_TYPE_CD as CLM_CTGY_TYPE_CD, CLM.ADDL_INSRNC_PLN_IND as ADDL_INSRNC_PLN_IND, CLM.EMPLMT_RLTD_IND as EMPLMT_RLTD_IND, CLM.ATTNY_INVLVMT_IND as ATTNY_INVLVMT_IND, CLM.CLM_PRIR_IND as CLM_PRIR_IND, CLM.PMT_MODE_CD as PMT_MODE_CD, CLM.CLM_OBLGTN_TYPE_CD as CLM_OBLGTN_TYPE_CD, CLM.SUBRGTN_ELGBL_CD as SUBRGTN_ELGBL_CD, CLM.SUBRGTN_ELGBLY_RSN_CD as SUBRGTN_ELGBLY_RSN_CD, CLM.CURY_CD as CURY_CD, CLM.INCDT_EV_ID as INCDT_EV_ID, CLM.INSRD_AT_FAULT_IND as INSRD_AT_FAULT_IND, CLM.CVGE_IN_QUES_IND as CVGE_IN_QUES_IND, CLM.EXTNT_OF_FIRE_DMG_TYPE_CD as EXTNT_OF_FIRE_DMG_TYPE_CD, CLM.VFYD_CLM_IND as VFYD_CLM_IND, CLM.PRCS_ID as PRCS_ID, CLM.CLM_STRT_DTTM as CLM_STRT_DTTM, CLM.CLM_END_DTTM as CLM_END_DTTM, CLM.EDW_STRT_DTTM as EDW_STRT_DTTM, CLM.EDW_END_DTTM as EDW_END_DTTM, CLM.TRANS_STRT_DTTM as TRANS_STRT_DTTM, CLM.LGCY_CLM_NUM as LGCY_CLM_NUM, CLM.CLM_NUM as CLM_NUM, CLM.SRC_SYS_CD as SRC_SYS_CD FROM DB_T_PROD_CORE.CLM  QUALIFY ROW_NUMBER() OVER(PARTITION BY CLM.CLM_NUM,CLM.SRC_SYS_CD  ORDER BY CLM.EDW_END_DTTM desc) = 1
) LKP ON LKP.CLM_NUM = exp_all_sources.claimnumber AND LKP.SRC_SYS_CD = exp_all_sources.out_CLM_SRC_CD
QUALIFY RNK = 1
);


-- Component LKP_STREETADDR, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_STREETADDR AS
(
SELECT
LKP.STREET_ADDR_ID,
LKP.CITY_ID,
LKP.TERR_ID,
LKP.POSTL_CD_ID,
LKP.CTRY_ID,
LKP.CNTY_ID,
sq_claim_associate_stag_x.source_record_id,
ROW_NUMBER() OVER(PARTITION BY sq_claim_associate_stag_x.source_record_id ORDER BY LKP.STREET_ADDR_ID asc,LKP.ADDR_LN_1_TXT asc,LKP.ADDR_LN_2_TXT asc,LKP.ADDR_LN_3_TXT asc,LKP.DWLNG_TYPE_CD asc,LKP.CITY_ID asc,LKP.TERR_ID asc,LKP.POSTL_CD_ID asc
--,LKP.TAX_LOC_ID asc
,LKP.CTRY_ID asc,LKP.CARIER_RTE_TXT asc,LKP.CNTY_ID asc,LKP.SPTL_PNT asc
--,LKP.LAT_MEAS asc
--,LKP.LNGTD_MEAS asc
,LKP.LOCTR_SBTYPE_CD asc,LKP.ADDR_SBTYPE_CD asc,LKP.GEOCODE_STS_TYPE_CD asc,LKP.ADDR_STDZN_TYPE_CD asc,LKP.PRCS_ID asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc) RNK1
FROM
sq_claim_associate_stag_x
INNER JOIN LKP_CTRY ON sq_claim_associate_stag_x.source_record_id = LKP_CTRY.source_record_id
INNER JOIN LKP_TERR ON LKP_CTRY.source_record_id = LKP_TERR.source_record_id
INNER JOIN LKP_POSTL_CD ON LKP_TERR.source_record_id = LKP_POSTL_CD.source_record_id
INNER JOIN LKP_CNTY ON LKP_POSTL_CD.source_record_id = LKP_CNTY.source_record_id
INNER JOIN LKP_CITY ON LKP_CNTY.source_record_id = LKP_CITY.source_record_id
LEFT JOIN (
SELECT STREET_ADDR.STREET_ADDR_ID as STREET_ADDR_ID, STREET_ADDR.DWLNG_TYPE_CD as DWLNG_TYPE_CD, STREET_ADDR.CARIER_RTE_TXT as CARIER_RTE_TXT, STREET_ADDR.SPTL_PNT as SPTL_PNT, STREET_ADDR.LOCTR_SBTYPE_CD as LOCTR_SBTYPE_CD, STREET_ADDR.ADDR_SBTYPE_CD as ADDR_SBTYPE_CD, STREET_ADDR.GEOCODE_STS_TYPE_CD as GEOCODE_STS_TYPE_CD, STREET_ADDR.ADDR_STDZN_TYPE_CD as ADDR_STDZN_TYPE_CD, STREET_ADDR.PRCS_ID as PRCS_ID, STREET_ADDR.EDW_STRT_DTTM as EDW_STRT_DTTM, STREET_ADDR.EDW_END_DTTM as EDW_END_DTTM, STREET_ADDR.ADDR_LN_1_TXT as ADDR_LN_1_TXT, STREET_ADDR.ADDR_LN_2_TXT as ADDR_LN_2_TXT, STREET_ADDR.ADDR_LN_3_TXT as ADDR_LN_3_TXT, STREET_ADDR.CITY_ID as CITY_ID, STREET_ADDR.TERR_ID as TERR_ID, STREET_ADDR.POSTL_CD_ID as POSTL_CD_ID, STREET_ADDR.CTRY_ID as CTRY_ID, STREET_ADDR.CNTY_ID as CNTY_ID FROM DB_T_PROD_CORE.STREET_ADDR qualify row_number () over (partition by ADDR_LN_1_TXT,ADDR_LN_2_TXT,ADDR_LN_3_TXT, CITY_ID ,TERR_ID,POSTL_CD_ID,CTRY_ID ,CNTY_ID order by EDW_END_DTTM desc)=1
) LKP ON LKP.ADDR_LN_1_TXT = sq_claim_associate_stag_x.Addline1 AND LKP.ADDR_LN_2_TXT = sq_claim_associate_stag_x.Addline2 
--AND LKP.ADDR_LN_3_TXT = in_ADDR_LN_3_TXT 
AND LKP.CITY_ID = LKP_CITY.CITY_ID AND LKP.TERR_ID = LKP_TERR.TERR_ID AND LKP.POSTL_CD_ID = LKP_POSTL_CD.POSTL_CD_ID AND LKP.CTRY_ID = LKP_CTRY.CTRY_ID AND LKP.CNTY_ID = LKP_CNTY.CNTY_ID
QUALIFY RNK1 = 1
);


-- Component exp_trans, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_trans AS
(
SELECT
LKP_TERR.TERR_ID as TERR_ID,
LKP_CNTY.CNTY_ID as CNTY_ID,
LKP_CITY.CITY_ID as CITY_ID,
LKP_POSTL_CD.POSTL_CD_ID as POSTL_CD_ID,
CONCAT ( exp_all_sources.locatorrolecode , exp_all_sources.locatortype ) as Concat_Code,
DECODE ( Concat_Code , ''JURLOCSTATE'' , LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_CLM_LOCTR_ROLE_CD */ , ''LOSSLOCSTATE'' , LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_CLM_LOCTR_ROLE_CD */ , ''LOSSLOCCOUNTY'' , LKP_3.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_CLM_LOCTR_ROLE_CD */ , ''LOSSLOCCITY'' , LKP_4.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_CLM_LOCTR_ROLE_CD */ , ''LOSSLOCZIP'' , LKP_5.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_CLM_LOCTR_ROLE_CD */ , ''LOSSLOCSTREET'' , LKP_6.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_CLM_LOCTR_ROLE_CD */ , ''MEDLOCSTREET'' , LKP_7.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_CLM_LOCTR_ROLE_CD */ , ''ARBLOCSTREET'' , LKP_8.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_CLM_LOCTR_ROLE_CD */ ) as src_CLM_LOCTR_ROLE_CD,
LKP_CLM.CLM_ID as src_CLM_ID,
DECODE ( Concat_Code , ''JURLOCSTATE'' , LKP_TERR.TERR_ID , ''LOSSLOCSTATE'' , LKP_TERR.TERR_ID , ''LOSSLOCCOUNTY'' , LKP_CNTY.CNTY_ID , ''LOSSLOCCITY'' , LKP_CITY.CITY_ID , ''LOSSLOCZIP'' , LKP_POSTL_CD.POSTL_CD_ID , ''LOSSLOCSTREET'' , LKP_STREETADDR.STREET_ADDR_ID , ''MEDLOCSTREET'' , LKP_STREETADDR.STREET_ADDR_ID , ''ARBLOCSTREET'' , LKP_STREETADDR.STREET_ADDR_ID ) as src_loc_id,
exp_all_sources.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_all_sources.EDW_END_DTTM as EDW_END_DTTM,
exp_all_sources.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
exp_all_sources.Retired as Retired,
exp_all_sources.PRCS_ID as PRCS_ID,
LKP_TERR.source_record_id,
row_number() over (partition by LKP_TERR.source_record_id order by LKP_TERR.source_record_id) as RNK1
FROM
LKP_TERR
INNER JOIN LKP_POSTL_CD ON LKP_TERR.source_record_id = LKP_POSTL_CD.source_record_id
INNER JOIN exp_all_sources ON LKP_POSTL_CD.source_record_id = exp_all_sources.source_record_id
INNER JOIN LKP_CNTY ON exp_all_sources.source_record_id = LKP_CNTY.source_record_id
INNER JOIN LKP_CITY ON LKP_CNTY.source_record_id = LKP_CITY.source_record_id
INNER JOIN LKP_CLM ON LKP_CITY.source_record_id = LKP_CLM.source_record_id
INNER JOIN LKP_STREETADDR ON LKP_CLM.source_record_id = LKP_STREETADDR.source_record_id
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_CLM_LOCTR_ROLE_CD LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = ''CLM_LOCTR_ROLE3''
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_CLM_LOCTR_ROLE_CD LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = ''CLM_LOCTR_ROLE9''
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_CLM_LOCTR_ROLE_CD LKP_3 ON LKP_3.SRC_IDNTFTN_VAL = ''CLM_LOCTR_ROLE10''
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_CLM_LOCTR_ROLE_CD LKP_4 ON LKP_4.SRC_IDNTFTN_VAL = ''CLM_LOCTR_ROLE8''
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_CLM_LOCTR_ROLE_CD LKP_5 ON LKP_5.SRC_IDNTFTN_VAL = ''CLM_LOCTR_ROLE11''
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_CLM_LOCTR_ROLE_CD LKP_6 ON LKP_6.SRC_IDNTFTN_VAL = ''CLM_LOCTR_ROLE12''
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_CLM_LOCTR_ROLE_CD LKP_7 ON LKP_7.SRC_IDNTFTN_VAL = ''CLM_LOCTR_ROLE5''
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_CLM_LOCTR_ROLE_CD LKP_8 ON LKP_8.SRC_IDNTFTN_VAL = ''CLM_LOCTR_ROLE1''
QUALIFY RNK1 = 1
);


-- Component LKP_CLM_LOC_ID, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_CLM_LOC_ID AS
(
SELECT
LKP.CLM_ID,
LKP.CLM_LOCTR_ROLE_CD,
LKP.LOC_ID,
LKP.EDW_STRT_DTTM,
LKP.EDW_END_DTTM,
LKP_CLM.source_record_id,
ROW_NUMBER() OVER(PARTITION BY LKP_CLM.source_record_id ORDER BY LKP.CLM_ID asc,LKP.CLM_LOCTR_ROLE_CD asc,LKP.LOC_ID asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc) RNK
FROM
LKP_CLM
INNER JOIN exp_trans ON LKP_CLM.source_record_id = exp_trans.source_record_id
LEFT JOIN (
SELECT CLM_LOCTR.CLM_LOCTR_ROLE_CD as CLM_LOCTR_ROLE_CD, CLM_LOCTR.LOC_ID as LOC_ID, CLM_LOCTR.EDW_STRT_DTTM as EDW_STRT_DTTM, CLM_LOCTR.EDW_END_DTTM as EDW_END_DTTM, CLM_LOCTR.CLM_ID as CLM_ID FROM DB_T_PROD_CORE.CLM_LOCTR
QUALIFY ROW_NUMBER() OVER(PARTITION BY CLM_LOCTR.CLM_LOCTR_ROLE_CD,CLM_LOCTR.CLM_ID ORDER BY EDW_END_DTTM desc) = 1
) LKP ON LKP.CLM_ID = LKP_CLM.CLM_ID AND LKP.CLM_LOCTR_ROLE_CD = exp_trans.src_CLM_LOCTR_ROLE_CD
QUALIFY RNK = 1
);


-- Component exp_ins_upd, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_ins_upd AS
(
SELECT
LKP_CLM_LOC_ID.CLM_ID as LKP_CLM_ID,
LKP_CLM_LOC_ID.CLM_LOCTR_ROLE_CD as LKP_CLM_LOCTR_ROLE_CD,
LKP_CLM_LOC_ID.EDW_STRT_DTTM as LKP_EDW_STRT_DTTM,
exp_trans.src_CLM_ID as src_CLM_ID,
exp_trans.src_CLM_LOCTR_ROLE_CD as src_CLM_LOCTR_ROLE_CD,
exp_trans.src_loc_id as src_loc_id,
exp_trans.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_trans.EDW_END_DTTM as EDW_END_DTTM,
exp_trans.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
exp_trans.Retired as Retired,
MD5 ( TO_CHAR ( LKP_CLM_LOC_ID.LOC_ID ) ) as LKP_MD5,
MD5 ( TO_CHAR ( exp_trans.src_loc_id ) ) as SRC_MD5,
CASE WHEN LKP_MD5 IS NULL THEN ''I'' ELSE CASE WHEN LKP_MD5 != SRC_MD5 THEN ''U'' ELSE ''R'' END END as OUT_INS_UPD,
LKP_CLM_LOC_ID.EDW_END_DTTM as LKP_EDW_END_DTTM,
exp_trans.PRCS_ID as PRCS_ID,
NULL as updatetime,
exp_trans.source_record_id
FROM
exp_trans
INNER JOIN LKP_CLM_LOC_ID ON exp_trans.source_record_id = LKP_CLM_LOC_ID.source_record_id
);


-- Component rtr_ins_upd_Insert, Type ROUTER Output Group Insert
CREATE OR REPLACE TEMPORARY TABLE rtr_ins_upd_Insert AS
(
SELECT
exp_ins_upd.src_CLM_ID as src_CLM_ID,
exp_ins_upd.src_CLM_LOCTR_ROLE_CD as src_CLM_LOCTR_ROLE_CD,
exp_ins_upd.src_loc_id as src_loc_id,
exp_ins_upd.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_ins_upd.EDW_END_DTTM as EDW_END_DTTM,
exp_ins_upd.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
exp_ins_upd.Retired as Retired,
exp_ins_upd.OUT_INS_UPD as OUT_INS_UPD,
exp_ins_upd.LKP_EDW_STRT_DTTM as LKP_EDW_STRT_DTTM,
exp_ins_upd.LKP_EDW_END_DTTM as LKP_EDW_END_DTTM,
exp_ins_upd.PRCS_ID as PRCS_ID,
exp_ins_upd.LKP_CLM_ID as LKP_CLM_ID,
exp_ins_upd.LKP_CLM_LOCTR_ROLE_CD as LKP_CLM_LOCTR_ROLE_CD,
exp_ins_upd.source_record_id
FROM
exp_ins_upd
WHERE exp_ins_upd.OUT_INS_UPD = ''I'' AND exp_ins_upd.src_loc_id IS NOT NULL AND exp_ins_upd.src_CLM_ID IS NOT NULL OR ( exp_ins_upd.LKP_EDW_END_DTTM != TO_timestamp ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) and exp_ins_upd.Retired = 0 ));


-- Component rtr_ins_upd_RETIRED, Type ROUTER Output Group RETIRED
CREATE OR REPLACE TEMPORARY TABLE rtr_ins_upd_RETIRED AS
(
SELECT
exp_ins_upd.src_CLM_ID as src_CLM_ID,
exp_ins_upd.src_CLM_LOCTR_ROLE_CD as src_CLM_LOCTR_ROLE_CD,
exp_ins_upd.src_loc_id as src_loc_id,
exp_ins_upd.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_ins_upd.EDW_END_DTTM as EDW_END_DTTM,
exp_ins_upd.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
exp_ins_upd.Retired as Retired,
exp_ins_upd.OUT_INS_UPD as OUT_INS_UPD,
exp_ins_upd.LKP_EDW_STRT_DTTM as LKP_EDW_STRT_DTTM,
exp_ins_upd.LKP_EDW_END_DTTM as LKP_EDW_END_DTTM,
exp_ins_upd.PRCS_ID as PRCS_ID,
exp_ins_upd.LKP_CLM_ID as LKP_CLM_ID,
exp_ins_upd.LKP_CLM_LOCTR_ROLE_CD as LKP_CLM_LOCTR_ROLE_CD,
exp_ins_upd.source_record_id
FROM
exp_ins_upd
WHERE exp_ins_upd.OUT_INS_UPD = ''R'' and exp_ins_upd.Retired != 0 and exp_ins_upd.LKP_EDW_END_DTTM = TO_timestamp( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ));


-- Component rtr_ins_upd_Update, Type ROUTER Output Group Update
CREATE OR REPLACE TEMPORARY TABLE rtr_ins_upd_Update AS
(
SELECT
exp_ins_upd.src_CLM_ID as src_CLM_ID,
exp_ins_upd.src_CLM_LOCTR_ROLE_CD as src_CLM_LOCTR_ROLE_CD,
exp_ins_upd.src_loc_id as src_loc_id,
exp_ins_upd.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_ins_upd.EDW_END_DTTM as EDW_END_DTTM,
exp_ins_upd.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
exp_ins_upd.Retired as Retired,
exp_ins_upd.OUT_INS_UPD as OUT_INS_UPD,
exp_ins_upd.LKP_EDW_STRT_DTTM as LKP_EDW_STRT_DTTM,
exp_ins_upd.LKP_EDW_END_DTTM as LKP_EDW_END_DTTM,
exp_ins_upd.PRCS_ID as PRCS_ID,
exp_ins_upd.LKP_CLM_ID as LKP_CLM_ID,
exp_ins_upd.LKP_CLM_LOCTR_ROLE_CD as LKP_CLM_LOCTR_ROLE_CD,
exp_ins_upd.source_record_id
FROM
exp_ins_upd
WHERE exp_ins_upd.OUT_INS_UPD = ''U'' AND exp_ins_upd.LKP_EDW_END_DTTM = TO_timestamp ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ));


-- Component upd_new_ins, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_new_ins AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_ins_upd_Insert.src_CLM_ID as src_CLM_ID1,
rtr_ins_upd_Insert.src_CLM_LOCTR_ROLE_CD as src_CLM_LOCTR_ROLE_CD1,
rtr_ins_upd_Insert.src_loc_id as src_loc_id1,
rtr_ins_upd_Insert.EDW_STRT_DTTM as EDW_STRT_DTTM1,
rtr_ins_upd_Insert.EDW_END_DTTM as EDW_END_DTTM1,
rtr_ins_upd_Insert.TRANS_STRT_DTTM as TRANS_STRT_DTTM1,
rtr_ins_upd_Insert.Retired as Retired1,
rtr_ins_upd_Insert.PRCS_ID as PRCS_ID1,
0 as UPDATE_STRATEGY_ACTION,
rtr_ins_upd_Insert.source_record_id
FROM
rtr_ins_upd_Insert
);


-- Component upd_update, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_update AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_ins_upd_Update.LKP_CLM_ID as LKP_CLM_ID3,
rtr_ins_upd_Update.LKP_CLM_LOCTR_ROLE_CD as LKP_CLM_LOCTR_ROLE_CD3,
rtr_ins_upd_Update.LKP_EDW_STRT_DTTM as LKP_EDW_STRT_DTTM3,
rtr_ins_upd_Update.EDW_STRT_DTTM as EDW_STRT_DTTM3,
rtr_ins_upd_Update.TRANS_STRT_DTTM as TRANS_STRT_DTTM3,
1 as UPDATE_STRATEGY_ACTION,
rtr_ins_upd_Update.SOURCE_RECORD_ID
FROM
rtr_ins_upd_Update
);


-- Component upd_retired, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_retired AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_ins_upd_RETIRED.LKP_CLM_ID as LKP_CLM_ID4,
rtr_ins_upd_RETIRED.LKP_CLM_LOCTR_ROLE_CD as LKP_CLM_LOCTR_ROLE_CD4,
rtr_ins_upd_RETIRED.LKP_EDW_STRT_DTTM as LKP_EDW_STRT_DTTM4,
rtr_ins_upd_RETIRED.TRANS_STRT_DTTM as TRANS_STRT_DTTM4,
rtr_ins_upd_RETIRED.source_record_id
FROM
rtr_ins_upd_RETIRED
);


-- Component exp_upd, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_upd AS
(
SELECT
upd_update.LKP_CLM_ID3 as LKP_CLM_ID3,
upd_update.LKP_CLM_LOCTR_ROLE_CD3 as LKP_CLM_LOCTR_ROLE_CD3,
upd_update.LKP_EDW_STRT_DTTM3 as LKP_EDW_STRT_DTTM3,
DATEADD(SECOND, -1, upd_update.EDW_STRT_DTTM3) as EDW_END_DTTM,
DATEADD(SECOND, -1, upd_update.TRANS_STRT_DTTM3) as TRANS_END_DTTM,
upd_update.source_record_id
FROM
upd_update
);


-- Component exp_new_ins, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_new_ins AS
(
SELECT
upd_new_ins.src_CLM_ID1 as src_CLM_ID1,
upd_new_ins.src_CLM_LOCTR_ROLE_CD1 as src_CLM_LOCTR_ROLE_CD1,
upd_new_ins.src_loc_id1 as src_loc_id1,
upd_new_ins.EDW_STRT_DTTM1 as EDW_STRT_DTTM1,
upd_new_ins.TRANS_STRT_DTTM1 as TRANS_STRT_DTTM1,
CASE WHEN upd_new_ins.Retired1 != 0 THEN upd_new_ins.TRANS_STRT_DTTM1 ELSE to_timestamp ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) END as TRANS_END_DTTM11,
CASE WHEN upd_new_ins.Retired1 = 0 THEN to_timestamp ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ELSE CURRENT_TIMESTAMP END as EDW_END_DTTM,
upd_new_ins.PRCS_ID1 as PRCS_ID1,
upd_new_ins.source_record_id
FROM
upd_new_ins
);


-- Component upd_ins, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_ins AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_ins_upd_Update.src_CLM_ID as src_CLM_ID3,
rtr_ins_upd_Update.src_CLM_LOCTR_ROLE_CD as src_CLM_LOCTR_ROLE_CD3,
rtr_ins_upd_Update.src_loc_id as src_loc_id3,
rtr_ins_upd_Update.EDW_STRT_DTTM as EDW_STRT_DTTM3,
rtr_ins_upd_Update.EDW_END_DTTM as EDW_END_DTTM3,
rtr_ins_upd_Update.TRANS_STRT_DTTM as TRANS_STRT_DTTM3,
rtr_ins_upd_Update.PRCS_ID as PRCS_ID3,
0 as UPDATE_STRATEGY_ACTION,
rtr_ins_upd_Update.SOURCE_RECORD_ID
FROM
rtr_ins_upd_Update
);


-- Component exp_retired, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_retired AS
(
SELECT
upd_retired.LKP_CLM_ID4 as LKP_CLM_ID4,
upd_retired.LKP_CLM_LOCTR_ROLE_CD4 as LKP_CLM_LOCTR_ROLE_CD4,
upd_retired.LKP_EDW_STRT_DTTM4 as LKP_EDW_STRT_DTTM4,
CURRENT_TIMESTAMP as EDW_END_DTTM,
upd_retired.TRANS_STRT_DTTM4 as TRANS_STRT_DTTM4,
upd_retired.source_record_id
FROM
upd_retired
);


-- Component CLM_LOCTR_retired, Type TARGET 
INSERT INTO DB_T_PROD_CORE.CLM_LOCTR
(
CLM_ID,
CLM_LOCTR_ROLE_CD,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_END_DTTM
)
SELECT
exp_retired.LKP_CLM_ID4 as CLM_ID,
exp_retired.LKP_CLM_LOCTR_ROLE_CD4 as CLM_LOCTR_ROLE_CD,
exp_retired.LKP_EDW_STRT_DTTM4 as EDW_STRT_DTTM,
exp_retired.EDW_END_DTTM as EDW_END_DTTM,
exp_retired.TRANS_STRT_DTTM4 as TRANS_END_DTTM
FROM
exp_retired;


-- Component CLM_LOCTR_upd, Type TARGET 
MERGE INTO DB_T_PROD_CORE.CLM_LOCTR
USING exp_upd ON (CLM_LOCTR.CLM_ID = exp_upd.LKP_CLM_ID3 AND CLM_LOCTR.CLM_LOCTR_ROLE_CD = exp_upd.LKP_CLM_LOCTR_ROLE_CD3 AND CLM_LOCTR.EDW_STRT_DTTM = exp_upd.LKP_EDW_STRT_DTTM3)
WHEN MATCHED THEN UPDATE
SET
CLM_ID = exp_upd.LKP_CLM_ID3,
CLM_LOCTR_ROLE_CD = exp_upd.LKP_CLM_LOCTR_ROLE_CD3,
EDW_STRT_DTTM = exp_upd.LKP_EDW_STRT_DTTM3,
EDW_END_DTTM = exp_upd.EDW_END_DTTM,
TRANS_END_DTTM = exp_upd.TRANS_END_DTTM;


-- Component CLM_LOCTR_new_ins, Type TARGET 
INSERT INTO DB_T_PROD_CORE.CLM_LOCTR
(
CLM_ID,
CLM_LOCTR_ROLE_CD,
LOC_ID,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM,
TRANS_END_DTTM
)
SELECT
exp_new_ins.src_CLM_ID1 as CLM_ID,
exp_new_ins.src_CLM_LOCTR_ROLE_CD1 as CLM_LOCTR_ROLE_CD,
exp_new_ins.src_loc_id1 as LOC_ID,
exp_new_ins.PRCS_ID1 as PRCS_ID,
exp_new_ins.EDW_STRT_DTTM1 as EDW_STRT_DTTM,
exp_new_ins.EDW_END_DTTM as EDW_END_DTTM,
exp_new_ins.TRANS_STRT_DTTM1 as TRANS_STRT_DTTM,
exp_new_ins.TRANS_END_DTTM11 as TRANS_END_DTTM
FROM
exp_new_ins;


-- Component exp_ins, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_ins AS
(
SELECT
upd_ins.src_CLM_ID3 as src_CLM_ID3,
upd_ins.src_CLM_LOCTR_ROLE_CD3 as src_CLM_LOCTR_ROLE_CD3,
upd_ins.src_loc_id3 as src_loc_id3,
upd_ins.EDW_STRT_DTTM3 as EDW_STRT_DTTM3,
upd_ins.EDW_END_DTTM3 as EDW_END_DTTM3,
upd_ins.TRANS_STRT_DTTM3 as TRANS_STRT_DTTM3,
upd_ins.PRCS_ID3 as PRCS_ID3,
upd_ins.source_record_id
FROM
upd_ins
);


-- Component CLM_LOCTR_ins, Type TARGET 
INSERT INTO DB_T_PROD_CORE.CLM_LOCTR
(
CLM_ID,
CLM_LOCTR_ROLE_CD,
LOC_ID,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM
)
SELECT
exp_ins.src_CLM_ID3 as CLM_ID,
exp_ins.src_CLM_LOCTR_ROLE_CD3 as CLM_LOCTR_ROLE_CD,
exp_ins.src_loc_id3 as LOC_ID,
exp_ins.PRCS_ID3 as PRCS_ID,
exp_ins.EDW_STRT_DTTM3 as EDW_STRT_DTTM,
exp_ins.EDW_END_DTTM3 as EDW_END_DTTM,
exp_ins.TRANS_STRT_DTTM3 as TRANS_STRT_DTTM
FROM
exp_ins;


END; ';