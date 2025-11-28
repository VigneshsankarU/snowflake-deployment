-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_CLM_EXPSR_INSUPD("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
declare
	start_dttm timestamp;
	end_dttm timestamp;
	prcs_id int;
	in_sys_src_cd VARCHAR;


BEGIN 

start_dttm := current_timestamp();
end_dttm := current_timestamp();
prcs_id := 1;
in_sys_src_cd := ''test'';

-- Component LKP_BUSN, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_BUSN AS
(
SELECT BUSN.BUSN_PRTY_ID as BUSN_PRTY_ID, BUSN.SRC_SYS_CD as SRC_SYS_CD, BUSN.TAX_BRAKT_CD as TAX_BRAKT_CD, BUSN.ORG_TYPE_CD as ORG_TYPE_CD, BUSN.GICS_SBIDSTRY_CD as GICS_SBIDSTRY_CD, BUSN.LIFCYCL_CD as LIFCYCL_CD, BUSN.PRTY_TYPE_CD as PRTY_TYPE_CD, BUSN.BUSN_END_DTTM as BUSN_END_DTTM, BUSN.BUSN_STRT_DTTM as BUSN_STRT_DTTM, BUSN.INC_IND as INC_IND, BUSN.EDW_STRT_DTTM as EDW_STRT_DTTM, BUSN.EDW_END_DTTM as EDW_END_DTTM, BUSN.BUSN_CTGY_CD as BUSN_CTGY_CD, BUSN.NK_BUSN_CD as NK_BUSN_CD 
FROM DB_T_PROD_CORE.BUSN 
QUALIFY ROW_NUMBER () OVER (PARTITION BY NK_BUSN_CD,BUSN_CTGY_CD ORDER BY EDW_END_DTTM DESC )=1
);


-- Component LKP_FEAT_ID, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_FEAT_ID AS
(
SELECT
FEAT_ID,
FEAT_SBTYPE_CD,
FEAT_INSRNC_SBTYPE_CD,
FEAT_CLASFCN_CD,
FEAT_DESC,
FEAT_NAME,
COMN_FEAT_NAME,
FEAT_LVL_SBTYPE_CNT,
INSRNC_CVGE_TYPE_CD,
INSRNC_LOB_TYPE_CD,
FEAT_DATA_TYPE_NAME,
FEAT_COVERABLE_TYPE_TXT,
PRCS_ID,
FEAT_DTL_MODL_TYPE_NAME,
FEAT_DTL_CD_NAME,
FEAT_DTL_VAL,
FEAT_DTL_VAL_TYPE,
FEAT_DTL_COL_NAME,
NK_SRC_KEY,
VAL_TYPE_CD
FROM DB_T_PROD_CORE.FEAT
);


-- Component LKP_INDIV_CLM_CTR, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_INDIV_CLM_CTR AS
(
SELECT 
	INDIV.INDIV_PRTY_ID as INDIV_PRTY_ID, 
	INDIV.NK_PUBLC_ID as NK_PUBLC_ID 
FROM 
	DB_T_PROD_CORE.INDIV
WHERE
	INDIV.NK_PUBLC_ID IS NOT NULL
);


-- Component LKP_INDIV_CNT_MGR, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_INDIV_CNT_MGR AS
(
SELECT 
	INDIV.INDIV_PRTY_ID as INDIV_PRTY_ID, 
	INDIV.NK_LINK_ID as NK_LINK_ID 
FROM 
	DB_T_PROD_CORE.INDIV
WHERE
	INDIV.NK_PUBLC_ID IS NULL
);


-- Component LKP_PRTY_ASSET_ID, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_PRTY_ASSET_ID AS
(
SELECT PRTY_ASSET.PRTY_ASSET_ID as PRTY_ASSET_ID, PRTY_ASSET.ASSET_INSRNC_HIST_TYPE_CD as ASSET_INSRNC_HIST_TYPE_CD, PRTY_ASSET.ASSET_DESC as ASSET_DESC, PRTY_ASSET.PRTY_ASSET_NAME as PRTY_ASSET_NAME, PRTY_ASSET.PRTY_ASSET_STRT_DTTM as PRTY_ASSET_STRT_DTTM, PRTY_ASSET.PRTY_ASSET_END_DTTM as PRTY_ASSET_END_DTTM, PRTY_ASSET.EDW_STRT_DTTM as EDW_STRT_DTTM, PRTY_ASSET.EDW_END_DTTM as EDW_END_DTTM, PRTY_ASSET.SRC_SYS_CD as SRC_SYS_CD, PRTY_ASSET.ASSET_HOST_ID_VAL as ASSET_HOST_ID_VAL, PRTY_ASSET.PRTY_ASSET_SBTYPE_CD as PRTY_ASSET_SBTYPE_CD, PRTY_ASSET.PRTY_ASSET_CLASFCN_CD as PRTY_ASSET_CLASFCN_CD 
FROM db_t_prod_core.PRTY_ASSET 
QUALIFY ROW_NUMBER() OVER(PARTITION BY  ASSET_HOST_ID_VAL,PRTY_ASSET_SBTYPE_CD,PRTY_ASSET_CLASFCN_CD ORDER BY EDW_END_DTTM DESC) = 1
);


-- Component LKP_TERADATA_ETL_REF_XLAT, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM IN (''LOSS_PRTY_TYPE'',''INSRNC_CVGE_TYPE'')

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM IN (''cctl_losspartytype.typecode'',''pc_etlclausepattern.coveragesubtype'')

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


-- Component LKP_TERADATA_ETL_REF_XLAT_BUSN_CTGY, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_BUSN_CTGY AS
(
SELECT 

 TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

 ,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

 db_t_prod_core.TERADATA_ETL_REF_XLAT

WHERE 

 TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM IN (''BUSN_CTGY'',''ORG_TYPE'',''PRTY_TYPE'')

 AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM IN (''derived'', ''cctl_contact.typecode'',''cctl_contact.name'')

 AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS IN (''DS'',''GW'')

 AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_CLM_EXPSR_TYPE, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_CLM_EXPSR_TYPE AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	db_t_prod_core.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM IN (''CLM_EXPSR_TYPE'')

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM IN (''cctl_exposuretype.typecode'')

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

	db_t_prod_core.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''SRC_SYS'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_XREF_CLM, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_XREF_CLM AS
(
SELECT
CLM_ID,
NK_SRC_KEY,
DIR_CLM_VAL
FROM db_t_prod_core.DIR_CLM
);


-- Component sq_cc_exposure, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_cc_exposure AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as PublicID,
$2 as CreateTime,
$3 as OtherCoverage,
$4 as ClaimNumber,
$5 as ClaimantPublicID,
$6 as incidenttype,
$7 as CotterFormInd_alfa,
$8 as Insrbl_int_key,
$9 as clausename,
$10 as clausetype,
$11 as feat_sbtype_cd,
$12 as Typecode,
$13 as CLM_SRC_CD,
$14 as CloseDate,
$15 as Retired,
$16 as assettype,
$17 as classification_type,
$18 as claimantprtytype,
$19 as Holdback_alfa,
$20 as HoldbackAmount_alfa,
$21 as HoldbackReimbursed_alfa,
$22 as RoofReplacement_alfa,
$23 as ExposureTypeCode,
$24 as Rank,
$25 as EDW_STRT_DTTM,
$26 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT 

cc_exposure.publicid,

cc_exposure.createtime,

cc_exposure.OtherCoverage, /* --EIM-32778 */
cc_exposure.claimnumber,

SUBSTR(cc_exposure.ClaimantPublicID, 1,  regexp_instr(cc_exposure.ClaimantPublicID, ''-'', 1)-1) as ClaimantPublicID,

cc_exposure.incidenttype,

cc_exposure.CotterFormInd_alfa,

cc_exposure.INSRBL_INT_KEY,

cc_exposure.Cov_Nk2,

cc_exposure.Cov_Nk3,

cc_exposure.Cov_Nk4,

cctl_losspartytype.typecode_stg typecode,

''SRC_SYS6'' AS CLM_SRC_CD,

cc_exposure.CloseDate,

cc_exposure.retired,

case when cc_exposure.incidenttype =10 then  ''PRTY_ASSET_SBTYPE11'' 

when cc_exposure.incidenttype =3 then ''PRTY_ASSET_SBTYPE4''   

when cc_exposure.incidenttype =11 then ''PRTY_ASSET_SBTYPE11''  else  ''PRTY_ASSET_SBTYPE5''  end as assettype,

case when cc_exposure.incidenttype =10 then   ''PRTY_ASSET_CLASFCN7'' 

when cc_exposure.incidenttype =3 then  ''PRTY_ASSET_CLASFCN3'' 

when cc_exposure.incidenttype =11 then ''homeowners'' else  ''PRTY_ASSET_CLASFCN1'' end as classification_type,

SUBSTR(cc_exposure.ClaimantPublicID,  regexp_instr(cc_exposure.ClaimantPublicID, ''-'', 1)+1) as clmntprtytype,

case when cc_exposure.holdback_alfa=1 then ''T'' when cc_exposure.holdback_alfa=0 then ''F'' else '''' end as holdback_alfa ,

cc_exposure.holdbackamount_alfa, 

case when cc_exposure.holdbackreimbursed_alfa=1 then ''T'' when cc_exposure.holdbackreimbursed_alfa=0 then ''F'' else '''' end as holdbackreimbursed_alfa , 

case when cc_exposure.roofreplacement_alfa=1 then ''T'' when cc_exposure.roofreplacement_alfa=0 then ''F'' else '''' end roofreplacement_alfa

,cc_exposure.ExposureTypeCode,Rnk as SRC_RNK,CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6)) as SRC_EDW_STRT_DTTM/* EIM-49209 */
FROM 

(

SELECT  distinct 

a.publicid_stg as Publicid,

a.createtime_stg as Createtime,

a.OtherCoverage_stg as OtherCoverage ,

b.claimnumber_stg as Claimnumber,

c.PublicID_stg || ''-''||cc.Name_stg as ClaimantPublicID,

e.Subtype as incidenttype,

case when  cc_evaluation.retired_stg=0  then cast(cc_evaluation.CotterFormInd_alfa_stg as varchar(5)) 

Else NULL

End as CotterFormInd_alfa,

insurable_key as INSRBL_INT_KEY,

f.clausename as Cov_Nk2,

f.clausetype as Cov_Nk3,

f.feat_sbtype_cd as Cov_Nk4,

a.CloseDate_stg as Closedate,

a.Retired_stg as Retired,

a.holdback_alfa_stg as holdback_alfa, 

a.holdbackamount_alfa_stg as holdbackamount_alfa,

a.holdbackreimbursed_alfa_stg as holdbackreimbursed_alfa,

a.roofreplacement_alfa_stg as roofreplacement_alfa,

expotype.typecode_stg as ExposureTypeCode,

a.LossParty_stg as LossParty,

Row_number() OVER(PARTITION BY a.publicid_stg ORDER BY a.updatetime_stg,a.createtime_stg,insurable_key) as Rnk/* EIM-49209 */
FROM

DB_T_PROD_STAG.cc_exposure a

inner join (select cc_claim.* from DB_T_PROD_STAG.cc_Claim inner join DB_T_PROD_STAG.cctl_claimstate on cc_claim.State_stg= cctl_claimstate.id_stg where cctl_claimstate.name_stg <> ''Draft'')  b on a.ClaimID_stg=b.ID_stg 

inner join DB_T_PROD_STAG.cctl_exposuretype expotype on expotype.ID_stg = a.ExposureType_stg and expotype.retired_stg=0/* EIM-17093 Adding new join to bring Exposuretypecode */
left outer join DB_T_PROD_STAG.cc_contact c on c.ID_stg = a.ClaimantDenormID_stg

left outer join DB_T_PROD_STAG.cctl_contact cc on c.subtype_stg = cc.id_stg

/************************** EIM-16161   NET LOSS REPORT  Taking the latest DB_T_PROD_STAG.cc_evaluation updatetime record ***********************************************/

LEFT OUTER JOIN (select cc_evaluation.*, rank() over(partition by exposureid_stg order by updatetime_stg desc) rnk  from DB_T_PROD_STAG.cc_evaluation ) cc_evaluation

 on a.id_stg = cc_evaluation.exposureid_stg 

and cc_evaluation.rnk=1 

left outer join 

(

select distinct cc_incident.id_stg as incid,cc_incident.subtype_stg as subtype, cctl_incident.typecode_stg as type1,

case 

when cctl_incident.typecode_stg = ''VehicleIncident'' then upper(insurable_key_veh)

when cctl_incident.typecode_stg = ''InjuryIncident'' then upper(insurable_key_inj) 

when cctl_incident.typecode_stg = ''FixedPropertyIncident'' or cctl_incident.typecode_stg = ''OtherStructureIncident'' then upper(insurable_key_dwell)

when cctl_incident.typecode_stg = ''DwellingIncident'' then upper(insurable_key_dwell_inc)

when cctl_incident.typecode_stg = ''PropertyContentsIncident'' or cctl_incident.typecode_stg = ''LivingExpensesIncident'' then upper(insurable_key_propcont)

end as insurable_key

from DB_T_PROD_STAG.CC_INCIDENT

left join DB_T_PROD_STAG.cctl_incident on cc_incident.Subtype_stg = cctl_incident.id_stg

left outer join

(select cc_contact.publicid_stg as insurable_key_inj,  

cc_claimcontactrole.IncidentID_stg 

from (select * from DB_T_PROD_STAG.cc_claimcontactrole where retired_stg = 0)  cc_claimcontactrole

join  DB_T_PROD_STAG.cc_claimcontact  on cc_claimcontactrole.ClaimContactID_stg = cc_claimcontact.id_stg

join DB_T_PROD_STAG.cc_contact on cc_claimcontact.ContactID_stg = cc_contact.ID_stg

join DB_T_PROD_STAG.cctl_contactrole on cctl_contactrole.id_stg=cc_claimcontactrole.role_stg                

and cctl_contactrole.typecode_stg=''injured''

) injuredpartydetails 

on cc_incident.id_stg = injuredpartydetails.IncidentID_stg

left outer join 

( 

select cc_vehicle.id_stg as id, 

case when PolicySystemId_stg is not null then SUBSTR(PolicySystemId_stg,POSITION('':''IN policysystemid_stg)+1,LENGTH(policysystemid_stg))

when (PolicySystemId_stg is null and Vin_stg is not null) then ''VIN:''||vin_stg 

when (PolicySystemId_stg is null and Vin_stg is null and LicensePlate_stg is not null) then ''LP:''||LicensePlate_stg 

when (PolicySystemId_stg is null and Vin_stg is null and LicensePlate_stg is null) then PublicID_stg

end as insurable_key_veh

from DB_T_PROD_STAG.cc_vehicle 

) veh on cc_incident.VehicleID_stg = veh.ID



/*  Avinash */
left outer join 

(

select cc_claim.ClaimNumber_stg As claimnumber, cc_incident.id_stg as id, cc_incident.subtype_stg as subtype, 

cctl_incident.name_stg as name_stg,  

cc_policylocation.policysystemid_stg as policysystemid,

cc_incident.Description_stg as Description,

cc_address.addressline1_stg as addressline1, 

cc_address.AddressLine2_stg as AddressLine2,  

case when policysystemid_stg is null then cc_incident.publicid_stg else 

SUBSTR(cc_policylocation.policysystemid_stg,POSITION('':'' IN cc_policylocation.policysystemid_stg)+1,LENGTH(cc_policylocation.policysystemid_stg)) end

as  insurable_key_dwell_inc

from  (select cc_claim.* from DB_T_PROD_STAG.cc_Claim inner join DB_T_PROD_STAG.cctl_claimstate on cc_claim.State_stg= cctl_claimstate.id_stg where cctl_claimstate.name_stg <> ''Draft'') cc_Claim 

left join DB_T_PROD_STAG.CC_INCIDENT on cc_claim.id_stg = cc_incident.ClaimID_stg

left join DB_T_PROD_STAG.cctl_incident on cc_incident.Subtype_stg = cctl_incident.id_stg

left join DB_T_PROD_STAG.CC_ADDRESS on cc_claim.LossLocationID_stg = cc_address.ID_stg

left join DB_T_PROD_STAG.cc_policylocation on cc_policylocation.AddressID_stg= cc_address.id_stg

where cctl_incident.name_stg =''DwellingIncident''

) Dwelling_inc on Dwelling_inc.ID=cc_incident.id_stg

left outer join 

(

select cc_claim.ClaimNumber_stg as claimnumber, cc_incident.id_stg as id, cc_incident.subtype_stg as subtype,

cctl_incident.name_stg as name_stg,

cc_policylocation.policysystemid_stg as policysystemid ,

cc_incident.Description_stg as Description,

cc_address.addressline1_stg as addressline1, 

cc_address.AddressLine2_stg as AddressLine2,  

cc_incident.publicid_stg as insurable_key_dwell

from (select cc_claim.* from DB_T_PROD_STAG.cc_Claim inner join DB_T_PROD_STAG.cctl_claimstate on cc_claim.State_stg= cctl_claimstate.id_stg where cctl_claimstate.name_stg <> ''Draft'') cc_Claim 

left join DB_T_PROD_STAG.CC_INCIDENT on cc_claim.id_stg = cc_incident.ClaimID_stg

left join DB_T_PROD_STAG.cctl_incident on cc_incident.Subtype_stg = cctl_incident.id_stg

left join DB_T_PROD_STAG.CC_ADDRESS on cc_claim.LossLocationID_stg = cc_address.ID_stg

left join DB_T_PROD_STAG.cc_policylocation on cc_policylocation.AddressID_stg = cc_address.id_stg

where cctl_incident.name_stg in (''OtherStructureIncident'',''FixedPropertyIncident'')

) Dwelling on Dwelling.ID=cc_incident.id_stg

left outer join 

(

Select ClaimNumber,id,subtype,name_stg,

policysystemid_stg,Description_stg,addressline1_stg,

AddressLine2_stg,

insurable_key_propcont 

FROM(/*  Added outer query as part of EIM- 20343, EIM-18973 */
select cc_claim.ClaimNumber_stg As Claimnumber, cc_incident.id_stg as id, 

cc_incident.subtype_stg as subtype , cctl_incident.name_stg as name_stg,  cc_riskunit.policysystemid_stg as policysystemid_stg, 

cc_incident.Description_stg as Description_stg , cc_address.addressline1_stg as addressline1_stg, 

cc_address.AddressLine2_stg as AddressLine2_stg,  

case when cc_riskunit.policysystemid_stg is null then cc_incident.publicid_stg else 

SUBSTR(cc_riskunit.policysystemid_stg,POSITION('':''IN cc_riskunit.policysystemid_stg)+1,LENGTH(cc_riskunit.policysystemid_stg)) end

as  insurable_key_propcont

,RANK() OVER(PARTITION BY CC_INCIDENT.ID_stg ORDER BY CC_INCIDENT.UPDATETIME_stg DESC,CC_RISKUNIT.UPDATETIME_stg DESC,CC_EXPOSURE.UPDATETIME_stg DESC,CC_COVERAGE.UPDATETIME_stg DESC, CC_ADDRESS.UPDATETIME_stg DESC) RNK   /*  Added as part of EIM-17289, Added exposure ,incident , coverage, DB_T_CORE_PROD.address updatetime as part of EIM-18973, 21273 */
from  (select cc_claim.* from DB_T_PROD_STAG.cc_Claim inner join DB_T_PROD_STAG.cctl_claimstate on cc_claim.State_stg= cctl_claimstate.id_stg where cctl_claimstate.name_stg <> ''Draft'') cc_Claim 

left join DB_T_PROD_STAG.cc_exposure on cc_claim.id_stg=cc_exposure.ClaimID_stg

left join DB_T_PROD_STAG.CC_INCIDENT on cc_exposure.IncidentID_stg = cc_incident.id_stg

left join DB_T_PROD_STAG.cc_coverage on cc_coverage.ID_stg=cc_exposure.CoverageID_stg

inner join DB_T_PROD_STAG.cc_riskunit on cc_riskunit.ID_stg=cc_coverage.RiskUnitID_stg

left join DB_T_PROD_STAG.cctl_incident on cc_incident.Subtype_stg = cctl_incident.id_stg

left join DB_T_PROD_STAG.CC_ADDRESS on cc_claim.LossLocationID_stg = cc_address.ID_stg

where cctl_incident.name_stg in (''PropertyContentsIncident'',''LivingExpensesIncident'')

) otr /*  Added outer query as part of EIM- 20343, EIM-18973 */
WHERE RNK=1 

/*  ------------------------------------ */
) PropertyContents on PropertyContents.id=cc_incident.id_stg

) e on a.IncidentID_stg = e.incid

/*  Avinash */
left outer join 

(

select 

distinct

claimnumber_stg as claimnumber, cc_exposure.id_stg as id,

SUBSTR(cc_coverage.PolicySystemId_stg,POSITION(''.'' IN cc_coverage.PolicySystemId_stg)+1,POSITION('':'' IN cc_coverage.PolicySystemId_stg)-POSITION(''.''IN cc_coverage.PolicySystemId_stg)-1) as CoverageSubtype, /*  INSRNC_CVGE_TYPE_CD */
cctl_coveragetype.typecode_stg as clausename, /* COMN_FEAT_NAME */
''COV'' as clausetype, /* FEAT_INSRNC_SBTYPE_CD */
''CL'' as feat_sbtype_cd

from (select cc_claim.* from DB_T_PROD_STAG.cc_Claim inner join DB_T_PROD_STAG.cctl_claimstate on cc_claim.State_stg= cctl_claimstate.id_stg where cctl_claimstate.name_stg <> ''Draft'') cc_Claim  

join DB_T_PROD_STAG.cc_exposure on cc_claim.id_stg=cc_exposure.claimid_stg

join DB_T_PROD_STAG.cc_coverage on cc_exposure.CoverageID_stg=cc_coverage.ID_stg

join DB_T_PROD_STAG.cctl_coveragetype on cc_coverage.Type_stg=cctl_coveragetype.ID_stg

/********  join DB_T_PROD_STAG.cctl_lobcode on cc_Claim.LOBCode_stg=cctl_lobcode.ID_stg   EIM-39436 ********/ 
) f on f.id=a.id_stg

WHERE  (a.UpdateTime_stg > (:start_dttm)    and a.UpdateTime_stg <= (:end_dttm)) OR

(cc_evaluation.UpdateTime_stg > (:start_dttm) and  cc_evaluation.UpdateTime_stg <= (:end_dttm))

) cc_exposure



LEFT OUTER JOIN DB_T_PROD_STAG.cctl_losspartytype 

ON cc_exposure.lossparty = cctl_losspartytype.id_stg
) SRC
)
);


-- Component exp_all_source_data, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_all_source_data AS
(
SELECT
sq_cc_exposure.PublicID as clm_exp_nk,
sq_cc_exposure.ClaimNumber as ClaimNumber,
sq_cc_exposure.CreateTime as CreateTime,
sq_cc_exposure.OtherCoverage as OtherCoverage,
sq_cc_exposure.CotterFormInd_alfa as CotterFormInd_alfa,
sq_cc_exposure.Retired as Retired,
sq_cc_exposure.HoldbackAmount_alfa as HoldbackAmount_alfa,
sq_cc_exposure.Rank as Rank,
sq_cc_exposure.EDW_STRT_DTTM as EDW_STRT_DTTM1,
CURRENT_TIMESTAMP as EDW_STRT_DTTM,
upper ( sq_cc_exposure.Insrbl_int_key ) as var_INSRBL_INT_KEY,
LKP_1.INDIV_PRTY_ID /* replaced lookup LKP_INDIV_CNT_MGR */ as var_lkp_indiv_prty_id_pc_bc,
LKP_2.INDIV_PRTY_ID /* replaced lookup LKP_INDIV_CLM_CTR */ as var_lkp_indiv_prty_id_cc,
LKP_3.PRTY_ASSET_ID /* replaced lookup LKP_PRTY_ASSET_ID */ as var_lkp_prty_asset_id,
LKP_4.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_BUSN_CTGY */ as out_lkp_busn_ctgy_cd,
CASE WHEN sq_cc_exposure.CreateTime IS NULL THEN to_date ( ''1900-01-01'' , ''YYYY-MM-DD'' ) ELSE sq_cc_exposure.CreateTime END as v_CLM_EXPSR_STRT_DT,
IFF (
  TRUE,
  CASE
    WHEN sq_cc_exposure.claimantprtytype IN (
      ''Person'',
      ''Adjudicator'',
      ''UserContact'',
      ''User Contact'',
      ''Vendor (Person)'',
      ''Attorney'',
      ''Doctor'',
      ''Policy Person'',
      ''Contact'',
      ''Lodging (Person)''
    ) THEN LKP_5.INDIV_PRTY_ID
    ELSE LKP_6.BUSN_PRTY_ID
  END,
  NULL
) AS out_lkp_clmnt_prty_id,
DECODE ( sq_cc_exposure.incidenttype , 5 , ''PERSON'' , ''ASSET'' ) as out_insrbl_int_ctgy_cd,
CASE WHEN var_lkp_indiv_prty_id_cc IS NULL THEN decode ( sq_cc_exposure.incidenttype , 5 , var_lkp_indiv_prty_id_pc_bc , var_lkp_prty_asset_id ) ELSE decode ( sq_cc_exposure.incidenttype , 5 , var_lkp_indiv_prty_id_cc , var_lkp_prty_asset_id ) END as out_prrty_or_asset_id,
DECODE ( TRUE , LKP_7.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT */ IS NULL , ''UNK'' , LKP_8.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT */ ) as TGT_IDNTFTN_VAL,
LKP_9.FEAT_ID /* replaced lookup LKP_FEAT_ID */ as out_feat_id,
LKP_10.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SRC_CD */ as out_CLM_SRC_CD,
v_CLM_EXPSR_STRT_DT as o_CLM_EXPSR_STRT_DT,
CASE WHEN sq_cc_exposure.CloseDate IS NULL THEN TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ELSE sq_cc_exposure.CloseDate END as o_CLM_EXPSR_END_DT,
CASE WHEN RTRIM ( sq_cc_exposure.Holdback_alfa ) = ''T'' THEN ''YES'' ELSE ''NO'' END as o_Holdback_alfa,
CASE WHEN RTRIM ( sq_cc_exposure.HoldbackReimbursed_alfa ) = ''T'' THEN ''YES'' ELSE ''NO'' END as o_HoldbackReimbursed_alfa,
CASE WHEN RTRIM ( sq_cc_exposure.RoofReplacement_alfa ) = ''T'' THEN ''YES'' ELSE ''NO'' END as o_RoofReplacement_alfa,
LKP_11.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_CLM_EXPSR_TYPE */ as out_ExposureTypeCode,
sq_cc_exposure.source_record_id,
row_number() over (partition by sq_cc_exposure.source_record_id order by sq_cc_exposure.source_record_id) as RNK
FROM
sq_cc_exposure
LEFT JOIN LKP_INDIV_CNT_MGR LKP_1 ON LKP_1.NK_LINK_ID = var_INSRBL_INT_KEY
LEFT JOIN LKP_INDIV_CLM_CTR LKP_2 ON LKP_2.NK_PUBLC_ID = var_INSRBL_INT_KEY
LEFT JOIN LKP_PRTY_ASSET_ID LKP_3 ON LKP_3.ASSET_HOST_ID_VAL = var_INSRBL_INT_KEY 
--AND LKP_3.PRTY_ASSET_SBTYPE_CD = :LKP.LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE ( sq_cc_exposure.assettype ) AND LKP_3.PRTY_ASSET_CLASFCN_CD = :LKP.LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN (sq_cc_exposure.classification_type )
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_BUSN_CTGY LKP_4 ON LKP_4.SRC_IDNTFTN_VAL = sq_cc_exposure.claimantprtytype
LEFT JOIN LKP_INDIV_CLM_CTR LKP_5 ON LKP_5.NK_PUBLC_ID = upper ( sq_cc_exposure.ClaimantPublicID )
LEFT JOIN LKP_BUSN LKP_6 ON LKP_6.BUSN_CTGY_CD = out_lkp_busn_ctgy_cd AND LKP_6.NK_BUSN_CD = sq_cc_exposure.ClaimantPublicID
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT LKP_7 ON LKP_7.SRC_IDNTFTN_VAL = sq_cc_exposure.Typecode
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT LKP_8 ON LKP_8.SRC_IDNTFTN_VAL = sq_cc_exposure.Typecode
LEFT JOIN LKP_FEAT_ID LKP_9 ON LKP_9.NK_SRC_KEY = sq_cc_exposure.clausename AND LKP_9.FEAT_INSRNC_SBTYPE_CD = sq_cc_exposure.clausetype AND LKP_9.FEAT_SBTYPE_CD = sq_cc_exposure.feat_sbtype_cd
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_SRC_CD LKP_10 ON LKP_10.SRC_IDNTFTN_VAL = sq_cc_exposure.CLM_SRC_CD
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_CLM_EXPSR_TYPE LKP_11 ON LKP_11.SRC_IDNTFTN_VAL = sq_cc_exposure.ExposureTypeCode
QUALIFY RNK = 1
);


-- Component LKP_INSRBL_INT_PRTY_ASSET_ID, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_INSRBL_INT_PRTY_ASSET_ID AS
(
SELECT
LKP.INSRBL_INT_ID,
exp_all_source_data.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_all_source_data.source_record_id ORDER BY LKP.INSRBL_INT_ID desc,LKP.INSRBL_INT_CTGY_CD desc,LKP.CTSTRPH_EXPSR_IND desc,LKP.SRC_SYS_CD desc,LKP.PRTY_ASSET_ID desc,LKP.EDW_STRT_DTTM desc,LKP.EDW_END_DTTM desc) RNK
FROM
exp_all_source_data
LEFT JOIN (
SELECT INSRBL_INT.INSRBL_INT_ID as INSRBL_INT_ID, INSRBL_INT.CTSTRPH_EXPSR_IND as CTSTRPH_EXPSR_IND, INSRBL_INT.EDW_STRT_DTTM as EDW_STRT_DTTM, INSRBL_INT.EDW_END_DTTM as EDW_END_DTTM, INSRBL_INT.INSRBL_INT_CTGY_CD as INSRBL_INT_CTGY_CD, INSRBL_INT.SRC_SYS_CD as SRC_SYS_CD, INSRBL_INT.PRTY_ASSET_ID as PRTY_ASSET_ID FROM db_t_prod_core.INSRBL_INT
 where  INSRBL_INT.INSRBL_INT_CTGY_CD = ''ASSET''
AND INSRBL_INT.PRTY_ASSET_ID IS NOT NULL
QUALIFY ROW_NUMBER() OVER(PARTITION BY INSRBL_INT_CTGY_CD,PRTY_ASSET_ID,SRC_SYS_CD  ORDER BY EDW_END_DTTM desc) = 1
) LKP ON LKP.INSRBL_INT_CTGY_CD = exp_all_source_data.out_insrbl_int_ctgy_cd AND LKP.SRC_SYS_CD = exp_all_source_data.out_CLM_SRC_CD AND LKP.PRTY_ASSET_ID = exp_all_source_data.out_prrty_or_asset_id
QUALIFY ROW_NUMBER() OVER(PARTITION BY exp_all_source_data.source_record_id ORDER BY LKP.INSRBL_INT_ID desc,LKP.INSRBL_INT_CTGY_CD desc,LKP.CTSTRPH_EXPSR_IND desc,LKP.SRC_SYS_CD desc,LKP.PRTY_ASSET_ID desc,LKP.EDW_STRT_DTTM desc,LKP.EDW_END_DTTM desc)  
= 1
);


-- Component LKP_CLM_EXPSR_ID, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_CLM_EXPSR_ID AS
(
SELECT
LKP.CLM_EXPSR_ID,
LKP.CLMNT_PRTY_ID,
LKP.CLM_EXPSR_RPTD_DTTM,
LKP.CLM_EXPSR_OTH_CARIER_CVGE_IND,
LKP.CLM_ID,
LKP.CVGE_FEAT_ID,
LKP.INSRBL_INT_ID,
LKP.COTTER_CLM_IND,
LKP.LOSS_PRTY_TYPE_CD,
LKP.HOLDBACK_IND,
LKP.HOLDBACK_AMT,
LKP.HOLDBACK_REIMBURSED_IND,
LKP.ROOF_RPLACEMT_IND,
LKP.CLM_EXPSR_TYPE_CD,
LKP.CLM_EXPSR_STRT_DTTM,
LKP.CLM_EXPSR_END_DTTM,
LKP.EDW_STRT_DTTM,
LKP.EDW_END_DTTM,
exp_all_source_data.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_all_source_data.source_record_id ORDER BY LKP.CLM_EXPSR_ID desc,LKP.CLMNT_PRTY_ID desc,LKP.CLM_EXPSR_NAME desc,LKP.CLM_EXPSR_RPTD_DTTM desc,LKP.CLM_EXPSR_OTH_CARIER_CVGE_IND desc,LKP.CLM_ID desc,LKP.CVGE_FEAT_ID desc,LKP.INSRBL_INT_ID desc,LKP.PRCS_ID desc,LKP.COTTER_CLM_IND desc,LKP.LOSS_PRTY_TYPE_CD desc,LKP.NK_SRC_KEY desc,LKP.HOLDBACK_IND desc,LKP.HOLDBACK_AMT desc,LKP.HOLDBACK_REIMBURSED_IND desc,LKP.ROOF_RPLACEMT_IND desc,LKP.CLM_EXPSR_TYPE_CD desc,LKP.CLM_EXPSR_STRT_DTTM desc,LKP.CLM_EXPSR_END_DTTM desc,LKP.EDW_STRT_DTTM desc,LKP.EDW_END_DTTM desc) RNK
FROM
exp_all_source_data
LEFT JOIN (
SELECT CLM_EXPSR.CLM_EXPSR_ID as CLM_EXPSR_ID, CLM_EXPSR.CLMNT_PRTY_ID as CLMNT_PRTY_ID, CLM_EXPSR.CLM_EXPSR_NAME as CLM_EXPSR_NAME, CLM_EXPSR.CLM_EXPSR_RPTD_DTTM as CLM_EXPSR_RPTD_DTTM, CLM_EXPSR.CLM_EXPSR_OTH_CARIER_CVGE_IND as CLM_EXPSR_OTH_CARIER_CVGE_IND, CLM_EXPSR.CLM_ID as CLM_ID, CLM_EXPSR.CVGE_FEAT_ID as CVGE_FEAT_ID, CLM_EXPSR.INSRBL_INT_ID as INSRBL_INT_ID, CLM_EXPSR.PRCS_ID as PRCS_ID, CLM_EXPSR.COTTER_CLM_IND as COTTER_CLM_IND, CLM_EXPSR.LOSS_PRTY_TYPE_CD as LOSS_PRTY_TYPE_CD, CLM_EXPSR.HOLDBACK_IND as HOLDBACK_IND , CLM_EXPSR.HOLDBACK_AMT as HOLDBACK_AMT, CLM_EXPSR.HOLDBACK_REIMBURSED_IND as HOLDBACK_REIMBURSED_IND, CLM_EXPSR.ROOF_RPLACEMT_IND as ROOF_RPLACEMT_IND, CLM_EXPSR.CLM_EXPSR_TYPE_CD AS CLM_EXPSR_TYPE_CD,CLM_EXPSR.CLM_EXPSR_STRT_DTTM as CLM_EXPSR_STRT_DTTM, CLM_EXPSR.CLM_EXPSR_END_DTTM as CLM_EXPSR_END_DTTM, CLM_EXPSR.EDW_STRT_DTTM as EDW_STRT_DTTM, CLM_EXPSR.EDW_END_DTTM as EDW_END_DTTM, CLM_EXPSR.NK_SRC_KEY as NK_SRC_KEY FROM db_t_prod_core.CLM_EXPSR 
QUALIFY ROW_NUMBER() OVER(PARTITION BY CLM_EXPSR.NK_SRC_KEY  ORDER BY CLM_EXPSR.EDW_END_DTTM DESC) = 1
) LKP ON LKP.NK_SRC_KEY = exp_all_source_data.clm_exp_nk
QUALIFY ROW_NUMBER() OVER(PARTITION BY exp_all_source_data.source_record_id ORDER BY LKP.CLM_EXPSR_ID desc,LKP.CLMNT_PRTY_ID desc,LKP.CLM_EXPSR_NAME desc,LKP.CLM_EXPSR_RPTD_DTTM desc,LKP.CLM_EXPSR_OTH_CARIER_CVGE_IND desc,LKP.CLM_ID desc,LKP.CVGE_FEAT_ID desc,LKP.INSRBL_INT_ID desc,LKP.PRCS_ID desc,LKP.COTTER_CLM_IND desc,LKP.LOSS_PRTY_TYPE_CD desc,LKP.NK_SRC_KEY desc,LKP.HOLDBACK_IND desc,LKP.HOLDBACK_AMT desc,LKP.HOLDBACK_REIMBURSED_IND desc,LKP.ROOF_RPLACEMT_IND desc,LKP.CLM_EXPSR_TYPE_CD desc,LKP.CLM_EXPSR_STRT_DTTM desc,LKP.CLM_EXPSR_END_DTTM desc,LKP.EDW_STRT_DTTM desc,LKP.EDW_END_DTTM desc)  
= 1
);


-- Component LKP_INSRBL_INT_PRTYID, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_INSRBL_INT_PRTYID AS
(
SELECT
LKP.INSRBL_INT_ID,
exp_all_source_data.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_all_source_data.source_record_id ORDER BY LKP.INSRBL_INT_ID desc,LKP.INSRBL_INT_CTGY_CD desc,LKP.CTSTRPH_EXPSR_IND desc,LKP.SRC_SYS_CD desc,LKP.INJURED_PRTY_ID desc,LKP.EDW_STRT_DTTM desc,LKP.EDW_END_DTTM desc) RNK
FROM
exp_all_source_data
LEFT JOIN (
SELECT INSRBL_INT.INSRBL_INT_ID as INSRBL_INT_ID, INSRBL_INT.CTSTRPH_EXPSR_IND as CTSTRPH_EXPSR_IND, INSRBL_INT.EDW_STRT_DTTM as EDW_STRT_DTTM, INSRBL_INT.EDW_END_DTTM as EDW_END_DTTM, INSRBL_INT.INSRBL_INT_CTGY_CD as INSRBL_INT_CTGY_CD, INSRBL_INT.SRC_SYS_CD as SRC_SYS_CD, INSRBL_INT.INJURED_PRTY_ID as INJURED_PRTY_ID FROM db_t_prod_core.INSRBL_INT
 where   INSRBL_INT.INSRBL_INT_CTGY_CD = ''PERSON''
AND  INSRBL_INT.INJURED_PRTY_ID  IS NOT NULL
QUALIFY ROW_NUMBER() OVER(PARTITION BY INSRBL_INT_CTGY_CD,INJURED_PRTY_ID,SRC_SYS_CD  ORDER BY EDW_END_DTTM desc) = 1
) LKP ON LKP.INSRBL_INT_CTGY_CD = exp_all_source_data.out_insrbl_int_ctgy_cd AND LKP.SRC_SYS_CD = :in_sys_src_cd AND LKP.INJURED_PRTY_ID = exp_all_source_data.out_prrty_or_asset_id
QUALIFY ROW_NUMBER() OVER(PARTITION BY exp_all_source_data.source_record_id ORDER BY LKP.INSRBL_INT_ID desc,LKP.INSRBL_INT_CTGY_CD desc,LKP.CTSTRPH_EXPSR_IND desc,LKP.SRC_SYS_CD desc,LKP.INJURED_PRTY_ID desc,LKP.EDW_STRT_DTTM desc,LKP.EDW_END_DTTM desc)  
= 1
);


-- Component LKP_CLM, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_CLM AS
(
SELECT
LKP.CLM_ID,
exp_all_source_data.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_all_source_data.source_record_id ORDER BY LKP.CLM_ID desc,LKP.CLM_TYPE_CD desc,LKP.CLM_MDIA_TYPE_CD desc,LKP.CLM_SUBMTL_TYPE_CD desc,LKP.ACDNT_TYPE_CD desc,LKP.CLM_CTGY_TYPE_CD desc,LKP.ADDL_INSRNC_PLN_IND desc,LKP.EMPLMT_RLTD_IND desc,LKP.ATTNY_INVLVMT_IND desc,LKP.CLM_NUM desc,LKP.CLM_PRIR_IND desc,LKP.PMT_MODE_CD desc,LKP.CLM_OBLGTN_TYPE_CD desc,LKP.SUBRGTN_ELGBL_CD desc,LKP.SUBRGTN_ELGBLY_RSN_CD desc,LKP.CURY_CD desc,LKP.INCDT_EV_ID desc,LKP.INSRD_AT_FAULT_IND desc,LKP.CVGE_IN_QUES_IND desc,LKP.EXTNT_OF_FIRE_DMG_TYPE_CD desc,LKP.VFYD_CLM_IND desc,LKP.PRCS_ID desc,LKP.CLM_STRT_DTTM desc,LKP.CLM_END_DTTM desc,LKP.EDW_STRT_DTTM desc,LKP.EDW_END_DTTM desc,LKP.SRC_SYS_CD desc,LKP.TRANS_STRT_DTTM desc,LKP.LGCY_CLM_NUM desc) RNK
FROM
exp_all_source_data
LEFT JOIN (
SELECT CLM.CLM_ID as CLM_ID, CLM.CLM_TYPE_CD as CLM_TYPE_CD, CLM.CLM_MDIA_TYPE_CD as CLM_MDIA_TYPE_CD, CLM.CLM_SUBMTL_TYPE_CD as CLM_SUBMTL_TYPE_CD, CLM.ACDNT_TYPE_CD as ACDNT_TYPE_CD, CLM.CLM_CTGY_TYPE_CD as CLM_CTGY_TYPE_CD, CLM.ADDL_INSRNC_PLN_IND as ADDL_INSRNC_PLN_IND, CLM.EMPLMT_RLTD_IND as EMPLMT_RLTD_IND, CLM.ATTNY_INVLVMT_IND as ATTNY_INVLVMT_IND, CLM.CLM_PRIR_IND as CLM_PRIR_IND, CLM.PMT_MODE_CD as PMT_MODE_CD, CLM.CLM_OBLGTN_TYPE_CD as CLM_OBLGTN_TYPE_CD, CLM.SUBRGTN_ELGBL_CD as SUBRGTN_ELGBL_CD, CLM.SUBRGTN_ELGBLY_RSN_CD as SUBRGTN_ELGBLY_RSN_CD, CLM.CURY_CD as CURY_CD, CLM.INCDT_EV_ID as INCDT_EV_ID, CLM.INSRD_AT_FAULT_IND as INSRD_AT_FAULT_IND, CLM.CVGE_IN_QUES_IND as CVGE_IN_QUES_IND, CLM.EXTNT_OF_FIRE_DMG_TYPE_CD as EXTNT_OF_FIRE_DMG_TYPE_CD, CLM.VFYD_CLM_IND as VFYD_CLM_IND, CLM.PRCS_ID as PRCS_ID, CLM.CLM_STRT_DTTM as CLM_STRT_DTTM, CLM.CLM_END_DTTM as CLM_END_DTTM, CLM.EDW_STRT_DTTM as EDW_STRT_DTTM, CLM.EDW_END_DTTM as EDW_END_DTTM, CLM.TRANS_STRT_DTTM as TRANS_STRT_DTTM, CLM.LGCY_CLM_NUM as LGCY_CLM_NUM, CLM.CLM_NUM as CLM_NUM, CLM.SRC_SYS_CD as SRC_SYS_CD FROM db_t_prod_core.CLM 
CLM  QUALIFY ROW_NUMBER() OVER(PARTITION BY CLM.CLM_NUM,CLM.SRC_SYS_CD  ORDER BY CLM.EDW_END_DTTM desc) = 1
) LKP ON LKP.CLM_NUM = exp_all_source_data.ClaimNumber AND LKP.SRC_SYS_CD = exp_all_source_data.out_CLM_SRC_CD
QUALIFY ROW_NUMBER() OVER(PARTITION BY exp_all_source_data.source_record_id ORDER BY LKP.CLM_ID desc,LKP.CLM_TYPE_CD desc,LKP.CLM_MDIA_TYPE_CD desc,LKP.CLM_SUBMTL_TYPE_CD desc,LKP.ACDNT_TYPE_CD desc,LKP.CLM_CTGY_TYPE_CD desc,LKP.ADDL_INSRNC_PLN_IND desc,LKP.EMPLMT_RLTD_IND desc,LKP.ATTNY_INVLVMT_IND desc,LKP.CLM_NUM desc,LKP.CLM_PRIR_IND desc,LKP.PMT_MODE_CD desc,LKP.CLM_OBLGTN_TYPE_CD desc,LKP.SUBRGTN_ELGBL_CD desc,LKP.SUBRGTN_ELGBLY_RSN_CD desc,LKP.CURY_CD desc,LKP.INCDT_EV_ID desc,LKP.INSRD_AT_FAULT_IND desc,LKP.CVGE_IN_QUES_IND desc,LKP.EXTNT_OF_FIRE_DMG_TYPE_CD desc,LKP.VFYD_CLM_IND desc,LKP.PRCS_ID desc,LKP.CLM_STRT_DTTM desc,LKP.CLM_END_DTTM desc,LKP.EDW_STRT_DTTM desc,LKP.EDW_END_DTTM desc,LKP.SRC_SYS_CD desc,LKP.TRANS_STRT_DTTM desc,LKP.LGCY_CLM_NUM desc)  
= 1
);


-- Component exp_data_transformation, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_data_transformation AS
(
SELECT
exp_all_source_data.out_lkp_clmnt_prty_id as CLMNT_PRTY_ID,
exp_all_source_data.CreateTime as CLM_EXPSR_RPTD_DT,
exp_all_source_data.OtherCoverage as CLM_EXPSR_OTH_CARIER_CVGE_IND,
exp_all_source_data.CotterFormInd_alfa as COTTER_CLM_IND,
exp_all_source_data.clm_exp_nk as NK_PUBLC_ID,
exp_all_source_data.out_feat_id as CVGE_FEAT_ID,
exp_all_source_data.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL,
LKP_CLM_EXPSR_ID.EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_all_source_data.o_CLM_EXPSR_STRT_DT as in_CLM_EXPSR_STRT_DT,
exp_all_source_data.o_CLM_EXPSR_END_DT as in_CLM_EXPSR_END_DT,
LKP_CLM_EXPSR_ID.EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_all_source_data.out_CLM_SRC_CD as out_CLM_SRC_CD,
exp_all_source_data.Retired as Retired,
exp_all_source_data.o_Holdback_alfa as Holdback_alfa,
exp_all_source_data.HoldbackAmount_alfa as HoldbackAmount_alfa,
exp_all_source_data.o_HoldbackReimbursed_alfa as HoldbackReimbursed_alfa,
exp_all_source_data.o_RoofReplacement_alfa as RoofReplacement_alfa,
exp_all_source_data.out_ExposureTypeCode as in_ExposureTypeCode,
exp_all_source_data.EDW_STRT_DTTM1 as EDW_STRT_DTTM,
exp_all_source_data.Rank as Rank,
DECODE ( exp_all_source_data.out_insrbl_int_ctgy_cd , ''PERSON'' , LKP_INSRBL_INT_PRTYID.INSRBL_INT_ID , ''ASSET'' , LKP_INSRBL_INT_PRTY_ASSET_ID.INSRBL_INT_ID ) as out_INSRBL_INT_ID1,
MD5 ( ltrim ( rtrim ( LKP_CLM_EXPSR_ID.CLM_EXPSR_RPTD_DTTM ) ) || ltrim ( rtrim ( LKP_CLM_EXPSR_ID.CLM_EXPSR_OTH_CARIER_CVGE_IND ) ) || ltrim ( rtrim ( LKP_CLM_EXPSR_ID.CVGE_FEAT_ID ) ) || ltrim ( rtrim ( LKP_CLM_EXPSR_ID.COTTER_CLM_IND ) ) || ltrim ( rtrim ( LKP_CLM_EXPSR_ID.INSRBL_INT_ID ) ) || ltrim ( rtrim ( LKP_CLM_EXPSR_ID.CLM_ID ) ) || ltrim ( rtrim ( LKP_CLM_EXPSR_ID.LOSS_PRTY_TYPE_CD ) ) || ltrim ( rtrim ( LKP_CLM_EXPSR_ID.HOLDBACK_IND ) ) || ltrim ( rtrim ( LKP_CLM_EXPSR_ID.HOLDBACK_AMT ) ) || ltrim ( rtrim ( LKP_CLM_EXPSR_ID.HOLDBACK_REIMBURSED_IND ) ) || ltrim ( rtrim ( LKP_CLM_EXPSR_ID.ROOF_RPLACEMT_IND ) ) || ltrim ( rtrim ( LKP_CLM_EXPSR_ID.CLM_EXPSR_TYPE_CD ) ) ) as chksum_lkp,
MD5 ( ltrim ( rtrim ( exp_all_source_data.CreateTime ) ) || ltrim ( rtrim ( exp_all_source_data.OtherCoverage ) ) || ltrim ( rtrim ( exp_all_source_data.out_feat_id ) ) || ltrim ( rtrim ( exp_all_source_data.CotterFormInd_alfa ) ) || ltrim ( rtrim ( out_INSRBL_INT_ID1 ) ) || ltrim ( rtrim ( LKP_CLM.CLM_ID ) ) || ltrim ( rtrim ( exp_all_source_data.TGT_IDNTFTN_VAL ) ) || ltrim ( rtrim ( exp_all_source_data.o_Holdback_alfa ) ) || ltrim ( rtrim ( exp_all_source_data.HoldbackAmount_alfa ) ) || ltrim ( rtrim ( exp_all_source_data.o_HoldbackReimbursed_alfa ) ) || ltrim ( rtrim ( exp_all_source_data.o_RoofReplacement_alfa ) ) || ltrim ( rtrim ( exp_all_source_data.out_ExposureTypeCode ) ) ) as chksum_inp,
LKP_CLM.CLM_ID as out_CLM_ID,
DECODE ( exp_all_source_data.out_insrbl_int_ctgy_cd , ''PERSON'' , LKP_INSRBL_INT_PRTYID.INSRBL_INT_ID , ''ASSET'' , LKP_INSRBL_INT_PRTY_ASSET_ID.INSRBL_INT_ID ) as out_INSRBL_INT_ID,
LKP_CLM_EXPSR_ID.CLM_EXPSR_ID as out_CLM_EXPSR_ID,
TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as EDW_END_DTTM,
dateadd (second, -1,  CURRENT_TIMESTAMP ) as EDW_END_DTTM_exp,
CASE WHEN chksum_lkp IS NULL THEN ''I'' ELSE CASE WHEN chksum_lkp != chksum_inp THEN ''U'' ELSE ''R'' END END as o_flag,
exp_all_source_data.source_record_id
FROM
exp_all_source_data
INNER JOIN LKP_INSRBL_INT_PRTY_ASSET_ID ON exp_all_source_data.source_record_id = LKP_INSRBL_INT_PRTY_ASSET_ID.source_record_id
INNER JOIN LKP_CLM_EXPSR_ID ON LKP_INSRBL_INT_PRTY_ASSET_ID.source_record_id = LKP_CLM_EXPSR_ID.source_record_id
INNER JOIN LKP_INSRBL_INT_PRTYID ON LKP_CLM_EXPSR_ID.source_record_id = LKP_INSRBL_INT_PRTYID.source_record_id
INNER JOIN LKP_CLM ON LKP_INSRBL_INT_PRTYID.source_record_id = LKP_CLM.source_record_id
);


-- Component rtr_clm_expsr_insupd_INSERT, Type ROUTER Output Group INSERT
create or replace temporary table rtr_clm_expsr_insupd_INSERT AS
SELECT
exp_data_transformation.out_CLM_ID as CLM_ID,
exp_data_transformation.CLMNT_PRTY_ID as CLMNT_PRTY_ID,
exp_data_transformation.CLM_EXPSR_RPTD_DT as CLM_EXPSR_RPTD_DT,
exp_data_transformation.out_INSRBL_INT_ID as INSRBL_INT_ID,
exp_data_transformation.CLM_EXPSR_OTH_CARIER_CVGE_IND as CLM_EXPSR_OTH_CARIER_CVGE_IND,
exp_data_transformation.COTTER_CLM_IND as COTTER_CLM_IND,
exp_data_transformation.CVGE_FEAT_ID as CVGE_FEAT_ID,
exp_data_transformation.NK_PUBLC_ID as NK_PUBLC_ID,
exp_data_transformation.out_CLM_EXPSR_ID as lkp_CLM_EXPSR_ID,
exp_data_transformation.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL,
exp_data_transformation.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_data_transformation.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_data_transformation.EDW_END_DTTM as EDW_END_DTTM,
exp_data_transformation.EDW_END_DTTM_exp as EDW_END_DTTM_exp,
exp_data_transformation.o_flag as o_flag,
exp_data_transformation.out_CLM_SRC_CD as out_CLM_SRC_CD,
exp_data_transformation.in_CLM_EXPSR_STRT_DT as CLM_EXPSR_STRT_DT,
exp_data_transformation.in_CLM_EXPSR_END_DT as in_CLM_EXPSR_END_DT,
exp_data_transformation.Retired as Retired,
exp_data_transformation.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_data_transformation.Holdback_alfa as Holdback_alfa,
exp_data_transformation.HoldbackAmount_alfa as HoldbackAmount_alfa,
exp_data_transformation.HoldbackReimbursed_alfa as HoldbackReimbursed_alfa,
exp_data_transformation.RoofReplacement_alfa as RoofReplacement_alfa,
exp_data_transformation.in_ExposureTypeCode as ExposureTypeCode,
exp_data_transformation.Rank as Rank,
exp_data_transformation.source_record_id
FROM
exp_data_transformation
WHERE ( exp_data_transformation.o_flag = ''I'' AND exp_data_transformation.out_CLM_ID IS NOT NULL OR ( exp_data_transformation.Retired = 0 AND exp_data_transformation.lkp_EDW_END_DTTM != TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ) ) or ( exp_data_transformation.o_flag = ''U'' AND exp_data_transformation.out_CLM_ID IS NOT NULL AND exp_data_transformation.out_CLM_EXPSR_ID IS NOT NULL );


-- Component rtr_clm_expsr_insupd_Retired, Type ROUTER Output Group Retired
create or replace temporary table rtr_clm_expsr_insupd_Retired AS
SELECT
exp_data_transformation.out_CLM_ID as CLM_ID,
exp_data_transformation.CLMNT_PRTY_ID as CLMNT_PRTY_ID,
exp_data_transformation.CLM_EXPSR_RPTD_DT as CLM_EXPSR_RPTD_DT,
exp_data_transformation.out_INSRBL_INT_ID as INSRBL_INT_ID,
exp_data_transformation.CLM_EXPSR_OTH_CARIER_CVGE_IND as CLM_EXPSR_OTH_CARIER_CVGE_IND,
exp_data_transformation.COTTER_CLM_IND as COTTER_CLM_IND,
exp_data_transformation.CVGE_FEAT_ID as CVGE_FEAT_ID,
exp_data_transformation.NK_PUBLC_ID as NK_PUBLC_ID,
exp_data_transformation.out_CLM_EXPSR_ID as lkp_CLM_EXPSR_ID,
exp_data_transformation.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL,
exp_data_transformation.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_data_transformation.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_data_transformation.EDW_END_DTTM as EDW_END_DTTM,
exp_data_transformation.EDW_END_DTTM_exp as EDW_END_DTTM_exp,
exp_data_transformation.o_flag as o_flag,
exp_data_transformation.out_CLM_SRC_CD as out_CLM_SRC_CD,
exp_data_transformation.in_CLM_EXPSR_STRT_DT as CLM_EXPSR_STRT_DT,
exp_data_transformation.in_CLM_EXPSR_END_DT as in_CLM_EXPSR_END_DT,
exp_data_transformation.Retired as Retired,
exp_data_transformation.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_data_transformation.Holdback_alfa as Holdback_alfa,
exp_data_transformation.HoldbackAmount_alfa as HoldbackAmount_alfa,
exp_data_transformation.HoldbackReimbursed_alfa as HoldbackReimbursed_alfa,
exp_data_transformation.RoofReplacement_alfa as RoofReplacement_alfa,
exp_data_transformation.in_ExposureTypeCode as ExposureTypeCode,
exp_data_transformation.Rank as Rank,
exp_data_transformation.source_record_id
FROM
exp_data_transformation
WHERE exp_data_transformation.o_flag = ''R'' and exp_data_transformation.Retired != 0 and exp_data_transformation.lkp_EDW_END_DTTM = TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' );


-- Component updstr_clm_expsr_ins, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE updstr_clm_expsr_ins AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_clm_expsr_insupd_INSERT.CLM_ID as CLM_ID,
rtr_clm_expsr_insupd_INSERT.CLMNT_PRTY_ID as CLMNT_PRTY_ID,
rtr_clm_expsr_insupd_INSERT.CLM_EXPSR_RPTD_DT as CLM_EXPSR_RPTD_DT,
rtr_clm_expsr_insupd_INSERT.INSRBL_INT_ID as INSRBL_INT_ID,
rtr_clm_expsr_insupd_INSERT.CLM_EXPSR_OTH_CARIER_CVGE_IND as CLM_EXPSR_OTH_CARIER_CVGE_IND,
rtr_clm_expsr_insupd_INSERT.COTTER_CLM_IND as COTTER_CLM_IND,
rtr_clm_expsr_insupd_INSERT.CVGE_FEAT_ID as CVGE_FEAT_ID,
rtr_clm_expsr_insupd_INSERT.NK_PUBLC_ID as NK_PUBLC_ID,
rtr_clm_expsr_insupd_INSERT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL1,
rtr_clm_expsr_insupd_INSERT.EDW_STRT_DTTM as EDW_STRT_DTTM1,
rtr_clm_expsr_insupd_INSERT.EDW_END_DTTM as EDW_END_DTTM1,
rtr_clm_expsr_insupd_INSERT.out_CLM_SRC_CD as out_CLM_SRC_CD1,
rtr_clm_expsr_insupd_INSERT.CLM_EXPSR_STRT_DT as CLM_EXPSR_STRT_DT1,
rtr_clm_expsr_insupd_INSERT.in_CLM_EXPSR_END_DT as in_CLM_EXPSR_END_DT1,
rtr_clm_expsr_insupd_INSERT.Retired as Retired1,
rtr_clm_expsr_insupd_INSERT.Holdback_alfa as Holdback_alfa1,
rtr_clm_expsr_insupd_INSERT.HoldbackAmount_alfa as HoldbackAmount_alfa1,
rtr_clm_expsr_insupd_INSERT.HoldbackReimbursed_alfa as HoldbackReimbursed_alfa1,
rtr_clm_expsr_insupd_INSERT.RoofReplacement_alfa as RoofReplacement_alfa1,
rtr_clm_expsr_insupd_INSERT.ExposureTypeCode as ExposureTypeCode1,
rtr_clm_expsr_insupd_INSERT.Rank as Rank1,
0 as UPDATE_STRATEGY_ACTION,
rtr_clm_expsr_insupd_INSERT.source_record_id
FROM
rtr_clm_expsr_insupd_INSERT
);


-- Component exp_pass_to_target_ins, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_target_ins AS
(
SELECT
updstr_clm_expsr_ins.CLM_ID as CLM_ID,
updstr_clm_expsr_ins.CLMNT_PRTY_ID as CLMNT_PRTY_ID,
updstr_clm_expsr_ins.CLM_EXPSR_RPTD_DT as CLM_EXPSR_RPTD_DT,
updstr_clm_expsr_ins.INSRBL_INT_ID as INSRBL_INT_ID,
updstr_clm_expsr_ins.CLM_EXPSR_OTH_CARIER_CVGE_IND as CLM_EXPSR_OTH_CARIER_CVGE_IND,
updstr_clm_expsr_ins.COTTER_CLM_IND as COTTER_CLM_IND,
updstr_clm_expsr_ins.CVGE_FEAT_ID as CVGE_FEAT_ID,
updstr_clm_expsr_ins.NK_PUBLC_ID as NK_PUBLC_ID,
updstr_clm_expsr_ins.TGT_IDNTFTN_VAL1 as TGT_IDNTFTN_VAL1,
updstr_clm_expsr_ins.out_CLM_SRC_CD1 as out_CLM_SRC_CD1,
updstr_clm_expsr_ins.CLM_EXPSR_STRT_DT1 as CLM_EXPSR_STRT_DT1,
updstr_clm_expsr_ins.in_CLM_EXPSR_END_DT1 as in_CLM_EXPSR_END_DT1,
updstr_clm_expsr_ins.Holdback_alfa1 as Holdback_alfa1,
updstr_clm_expsr_ins.HoldbackAmount_alfa1 as HoldbackAmount_alfa1,
updstr_clm_expsr_ins.HoldbackReimbursed_alfa1 as HoldbackReimbursed_alfa1,
updstr_clm_expsr_ins.RoofReplacement_alfa1 as RoofReplacement_alfa1,
updstr_clm_expsr_ins.ExposureTypeCode1 as ExposureTypeCode1,
:PRCS_ID as PRCS_ID,
LKP_1.CLM_ID /* replaced lookup LKP_XREF_CLM */ as CLM_EXPSR_ID,
CASE WHEN updstr_clm_expsr_ins.Retired1 != 0 THEN updstr_clm_expsr_ins.EDW_STRT_DTTM1 ELSE updstr_clm_expsr_ins.EDW_END_DTTM1 END as o_EDW_END_DTTM1,
dateadd ( second, ( 2 * ( updstr_clm_expsr_ins.Rank1 - 1 ) ),updstr_clm_expsr_ins.EDW_STRT_DTTM1 ) as o_EDW_STRT_DTTM,
updstr_clm_expsr_ins.source_record_id,
row_number() over (partition by updstr_clm_expsr_ins.source_record_id order by updstr_clm_expsr_ins.source_record_id) as RNK
FROM
updstr_clm_expsr_ins
LEFT JOIN LKP_XREF_CLM LKP_1 ON LKP_1.NK_SRC_KEY = ltrim ( rtrim ( updstr_clm_expsr_ins.NK_PUBLC_ID ) ) AND LKP_1.DIR_CLM_VAL = ''CLMEXPSR''
QUALIFY row_number() over (partition by updstr_clm_expsr_ins.source_record_id order by updstr_clm_expsr_ins.source_record_id) 
= 1
);


-- Component updstr_clm_expsr_retired_rejected, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE updstr_clm_expsr_retired_rejected AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_clm_expsr_insupd_Retired.lkp_CLM_EXPSR_ID as lkp_CLM_EXPSR_ID4,
rtr_clm_expsr_insupd_Retired.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM4,
1 as UPDATE_STRATEGY_ACTION,
rtr_clm_expsr_insupd_Retired.source_record_id as source_record_id
FROM
rtr_clm_expsr_insupd_Retired
);


-- Component tgt_clm_expsr_ins, Type TARGET 
INSERT INTO DB_T_PROD_CORE.CLM_EXPSR
(
CLM_EXPSR_ID,
CLMNT_PRTY_ID,
CLM_EXPSR_RPTD_DTTM,
CLM_EXPSR_OTH_CARIER_CVGE_IND,
CLM_ID,
CVGE_FEAT_ID,
INSRBL_INT_ID,
NK_SRC_KEY,
COTTER_CLM_IND,
LOSS_PRTY_TYPE_CD,
HOLDBACK_IND,
HOLDBACK_AMT,
HOLDBACK_REIMBURSED_IND,
ROOF_RPLACEMT_IND,
CLM_EXPSR_TYPE_CD,
PRCS_ID,
CLM_EXPSR_STRT_DTTM,
CLM_EXPSR_END_DTTM,
SRC_SYS_CD,
EDW_STRT_DTTM,
EDW_END_DTTM
)
SELECT
exp_pass_to_target_ins.CLM_EXPSR_ID as CLM_EXPSR_ID,
exp_pass_to_target_ins.CLMNT_PRTY_ID as CLMNT_PRTY_ID,
exp_pass_to_target_ins.CLM_EXPSR_RPTD_DT as CLM_EXPSR_RPTD_DTTM,
exp_pass_to_target_ins.CLM_EXPSR_OTH_CARIER_CVGE_IND as CLM_EXPSR_OTH_CARIER_CVGE_IND,
exp_pass_to_target_ins.CLM_ID as CLM_ID,
exp_pass_to_target_ins.CVGE_FEAT_ID as CVGE_FEAT_ID,
exp_pass_to_target_ins.INSRBL_INT_ID as INSRBL_INT_ID,
exp_pass_to_target_ins.NK_PUBLC_ID as NK_SRC_KEY,
exp_pass_to_target_ins.COTTER_CLM_IND as COTTER_CLM_IND,
exp_pass_to_target_ins.TGT_IDNTFTN_VAL1 as LOSS_PRTY_TYPE_CD,
exp_pass_to_target_ins.Holdback_alfa1 as HOLDBACK_IND,
exp_pass_to_target_ins.HoldbackAmount_alfa1 as HOLDBACK_AMT,
exp_pass_to_target_ins.HoldbackReimbursed_alfa1 as HOLDBACK_REIMBURSED_IND,
exp_pass_to_target_ins.RoofReplacement_alfa1 as ROOF_RPLACEMT_IND,
exp_pass_to_target_ins.ExposureTypeCode1 as CLM_EXPSR_TYPE_CD,
exp_pass_to_target_ins.PRCS_ID as PRCS_ID,
exp_pass_to_target_ins.CLM_EXPSR_STRT_DT1 as CLM_EXPSR_STRT_DTTM,
exp_pass_to_target_ins.in_CLM_EXPSR_END_DT1 as CLM_EXPSR_END_DTTM,
exp_pass_to_target_ins.out_CLM_SRC_CD1 as SRC_SYS_CD,
exp_pass_to_target_ins.o_EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_pass_to_target_ins.o_EDW_END_DTTM1 as EDW_END_DTTM
FROM
exp_pass_to_target_ins;


-- Component tgt_clm_expsr_ins, Type Post SQL 
UPDATE  db_t_prod_core.CLM_EXPSR  
set EDW_END_DTTM=A.lead1

FROM

(SELECT	distinct  CLM_EXPSR_ID,EDW_STRT_DTTM,

max(EDW_STRT_DTTM) over (partition by  CLM_EXPSR_ID ORDER BY EDW_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND'' 

 as lead1

FROM	db_t_prod_core.CLM_EXPSR

 ) a


where  CLM_EXPSR.EDW_STRT_DTTM = A.EDW_STRT_DTTM

and CLM_EXPSR.CLM_EXPSR_ID=A.CLM_EXPSR_ID 

and CAST(CLM_EXPSR.EDW_END_DTTM AS DATE)=''9999-12-31''

and lead1 is not null;


-- Component exp_pass_to_target_Upd_retired_rejected, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_target_Upd_retired_rejected AS
(
SELECT
updstr_clm_expsr_retired_rejected.lkp_CLM_EXPSR_ID4 as lkp_CLM_EXPSR_ID4,
updstr_clm_expsr_retired_rejected.lkp_EDW_STRT_DTTM4 as lkp_EDW_STRT_DTTM4,
updstr_clm_expsr_retired_rejected.lkp_EDW_STRT_DTTM4 as EDW_END_DTTM,
updstr_clm_expsr_retired_rejected.source_record_id
FROM
updstr_clm_expsr_retired_rejected
);


-- Component tgt_clm_expsr_upd_retired_rejected, Type TARGET 
MERGE INTO DB_T_PROD_CORE.CLM_EXPSR
USING exp_pass_to_target_Upd_retired_rejected ON (CLM_EXPSR.CLM_EXPSR_ID=exp_pass_to_target_Upd_retired_rejected.lkp_CLM_EXPSR_ID4)
WHEN MATCHED THEN UPDATE
SET
CLM_EXPSR_ID = exp_pass_to_target_Upd_retired_rejected.lkp_CLM_EXPSR_ID4,
EDW_STRT_DTTM = exp_pass_to_target_Upd_retired_rejected.lkp_EDW_STRT_DTTM4,
EDW_END_DTTM = exp_pass_to_target_Upd_retired_rejected.EDW_END_DTTM;


END; ';