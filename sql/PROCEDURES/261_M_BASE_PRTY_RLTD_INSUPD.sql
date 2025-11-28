-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_PRTY_RLTD_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
DECLARE
       run_id STRING;
       start_dttm TIMESTAMP;
       end_dttm TIMESTAMP;
       prcs_id STRING;
	   v_start_time TIMESTAMP;
BEGIN
       run_id := (SELECT run_id FROM control_run_id WHERE worklet_name = :worklet_name ORDER BY insert_ts DESC LIMIT 1);
       start_dttm := (SELECT param_value FROM control_params WHERE run_id = :run_id AND param_name = ''start_dttm'' LIMIT 1);
       end_dttm := (SELECT param_value FROM control_params WHERE run_id = :run_id AND param_name = ''end_dttm'' LIMIT 1);
       prcs_id := (SELECT param_value FROM control_params WHERE run_id = :run_id AND param_name = ''WKLT_PRCS_ID'' LIMIT 1);
	   v_start_time := CURRENT_TIMESTAMP();

-- Component LKP_BUSN, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_BUSN AS
(
SELECT BUSN.BUSN_PRTY_ID as BUSN_PRTY_ID, BUSN.SRC_SYS_CD as SRC_SYS_CD, BUSN.TAX_BRAKT_CD as TAX_BRAKT_CD, BUSN.ORG_TYPE_CD as ORG_TYPE_CD, BUSN.GICS_SBIDSTRY_CD as GICS_SBIDSTRY_CD, BUSN.LIFCYCL_CD as LIFCYCL_CD, BUSN.PRTY_TYPE_CD as PRTY_TYPE_CD, BUSN.BUSN_END_DTTM as BUSN_END_DTTM, BUSN.BUSN_STRT_DTTM as BUSN_STRT_DTTM, BUSN.INC_IND as INC_IND, BUSN.EDW_STRT_DTTM as EDW_STRT_DTTM, BUSN.EDW_END_DTTM as EDW_END_DTTM, BUSN.BUSN_CTGY_CD as BUSN_CTGY_CD, BUSN.NK_BUSN_CD as NK_BUSN_CD 
FROM DB_T_PROD_CORE.BUSN 
QUALIFY ROW_NUMBER () OVER (PARTITION BY NK_BUSN_CD,BUSN_CTGY_CD ORDER BY EDW_END_DTTM DESC )=1
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


-- Component LKP_INTRNL_ORG, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_INTRNL_ORG AS
(
SELECT	INTRNL_ORG.INTRNL_ORG_PRTY_ID as INTRNL_ORG_PRTY_ID, INTRNL_ORG.INTRNL_ORG_TYPE_CD as INTRNL_ORG_TYPE_CD,
		INTRNL_ORG.INTRNL_ORG_SBTYPE_CD as INTRNL_ORG_SBTYPE_CD, INTRNL_ORG.INTRNL_ORG_NUM as INTRNL_ORG_NUM,
		INTRNL_ORG.SRC_SYS_CD as SRC_SYS_CD 
FROM	DB_T_PROD_CORE.INTRNL_ORG 
 qualify row_number () over (partition by INTRNL_ORG_NUM,INTRNL_ORG_TYPE_CD,INTRNL_ORG_SBTYPE_CD,SRC_SYS_CD order by EDW_END_DTTM desc)=1
);


-- Component LKP_PRTY_RLTD, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_PRTY_RLTD AS
(
SELECT
PRTY_ID,
RLTD_PRTY_ID,
PRTY_RLTD_ROLE_CD
FROM DB_T_PROD_CORE.PRTY_RLTD
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

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM IN (''BUSN_CTGY'',''PRTY_RLTD_ROLE'' )

     	AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM IN (''derived'',''pctl_relationship.typecode'')

	AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS IN (''DS'',''GW'')

	AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_INTRNL_ORG_SBTYPE, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_INTRNL_ORG_SBTYPE AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

		TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''INTRNL_ORG_SBTYPE''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_INTRNL_ORG_TYPE, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_INTRNL_ORG_TYPE AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''INTRNL_ORG_TYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_PRTY_STRC, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_PRTY_STRC AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM IN (''PRTY_STRC_TYPE'' )

     	AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM IN (''derived'')

	AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS IN (''DS'')

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


-- PIPELINE START FOR 1

-- Component SQ_pc_prty_rltd, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_pc_prty_rltd AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as Relationship,
$2 as PNI_AddressBookUID,
$3 as OTHER_AddressBookUID,
$4 as Code,
$5 as Name,
$6 as EffectiveDate,
$7 as ExpirationDate,
$8 as Lookups,
$9 as SYS_SRC_CD,
$10 as updatetime,
$11 as Retired,
$12 as Party_Structure_Type_Cd,
$13 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
/*********************************** Claims Hierarchy Start***********************************/

/** Use INT ORG to get parent and child PARTY ID***/

 SELECT DISTINCT

	Relationship as Relationship, 

	PNI_AddressBookUID as PNI_AddressBookUID, 

	OTHER_AddressBookUID as OTHER_AddressBookUID, 

	Code as Code, 

	Name1 as Name1, 

	 eff_dt  as EffectiveDate, 

	 end_dt as ExpirationDate, 

	Lookups as Lookups,

	''SRC_SYS4'' AS SYS_SRC_CD,

	updatetime,

	 Retired,Party_Structure_Type_Cd from (

SELECT DISTINCT 

cast(case when cct.TYPECODE_stg=''district_alfa'' and childtype.TYPECODE_stg=''servicecenter_alfa'' then ''PRTY_RLTD_ROLE24''

when cct.TYPECODE_stg=''district_alfa'' and childtype.TYPECODE_stg=''stormcenter_alfa'' then ''PRTY_RLTD_ROLE41''

when cct.TYPECODE_stg=''general'' and childtype.TYPECODE_stg=''general'' then ''PRTY_RLTD_ROLE42''

when cct.TYPECODE_stg=''general'' and childtype.TYPECODE_stg=''medical_mgmt'' then ''PRTY_RLTD_ROLE44''

when cct.TYPECODE_stg=''general'' and childtype.TYPECODE_stg=''region_alfa'' then ''PRTY_RLTD_ROLE43''

when cct.TYPECODE_stg=''region_alfa'' and childtype.TYPECODE_stg=''district_alfa'' then ''PRTY_RLTD_ROLE23''

when cct.TYPECODE_stg=''region_alfa'' and childtype.TYPECODE_stg=''general'' then ''PRTY_RLTD_ROLE45''

when cct.TYPECODE_stg=''root'' and childtype.TYPECODE_stg=''general'' then ''PRTY_RLTD_ROLE46''

when cct.TYPECODE_stg=''district_alfa'' and childtype.TYPECODE_stg=''general'' then ''PRTY_RLTD_ROLE47''

else  NULL end as varchar(50)) as Relationship,

/* ''PRTY_RLTD_ROLE4'' as Relationship, -- Need to add value in XLAT table */
cast(cct.TYPECODE_stg as varchar(100)) AS PNI_AddressBookUID,

cast(ccg.name_stg as varchar(100))AS OTHER_AddressBookUID,

cast(childtype.TYPECODE_stg as varchar(100)) AS Code,

child.name_stg AS Name1,

CAST(''1990-01-01 00:00:00.000000'' AS timestamp(6)) AS eff_dt, /*  Need to follow up with Ahmed			 */
CAST(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS end_dt, /*  Need to follow up with Ahmed		 */
cast(current_timestamp as timestamp(6))  AS UpdateTime,

cast(''INTRNL_ORG'' as varchar(50))as Lookups,  

 case when ccg.retired_stg=0 and child.retired_stg=0 and cct.retired_stg=0 and childtype.retired_stg=0 then 0 else 1 end as Retired,

cast(''PRTY_STRC_TYPE15'' as varchar(50)) AS Party_Structure_Type_Cd

FROM

DB_T_PROD_STAG.cc_parentgroup ccp inner join DB_T_PROD_STAG.cc_group ccg on ccg.id_stg=ccp.ForeignEntityID_stg

inner join DB_T_PROD_STAG.cctl_grouptype cct on cct.id_stg=ccg.GroupType_stg

inner join DB_T_PROD_STAG.cc_group child on child.id_stg=ccp.OwnerID_stg

inner join DB_T_PROD_STAG.cctl_grouptype childtype on childtype.id_stg=child.GroupType_stg

where not (cct.TYPECODE_stg = ''region_alfa'' and childtype.TYPECODE_stg = ''servicecenter_alfa'')

 union

/**District or Storm Center associated with the adjuster relationship****/

SELECT DISTINCT

	''PRTY_RLTD_ROLE12'' AS Relationship, 

	cctl_grouptypecode,

	cc_group_name AS PNI_AddressBookUID,

	cc_contact_publicID AS OTHER_AddressBookUID,

	cast(NULL as varchar(30)) AS Name1,

	CAST(''1900-01-01 00:00:00.000000'' AS timestamp(6)) AS eff_dt, 

	CAST(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS END_dt, 

	CAST(''1900-01-01 00:00:00.000000'' AS timestamp(6)) AS UpdateTime,

	''INTRNL_ORG Claim_INDIV'' AS Lookups, 

	CASE WHEN usersgroup_retired=0 and cc_contact_retired=0  then 0 else 1 END AS Retired,

	''PRTY_STRC_TYPE9''  AS Party_Structure_Type_Cd

	FROM 

	(SELECT 

			cgu.UserID_stg AS adjusterID,

			cct.TYPECODE_stg AS cctl_grouptypecode,

			cg.Name_stg AS cc_group_name,

			cc.PublicID_stg AS cc_contact_publicID,

			cg.retired_stg AS usersgroup_retired,

			cc.retired_stg AS cc_contact_retired,

			rank() over (Partition by cu.AdjusterCode_Alfa_stg order by CASE WHEN left(cu.adjusterCode_alfa_stg,1)=  CASE WHEN cct.Name_stg = ''District'' then left(cg.name_stg,1)

 			END or right(cu.adjusterCode_alfa_stg,1)= CASE WHEN cct.Name_stg = ''District'' then left(cg.name_stg,1)

END  then 1 else 2 END) AS rankDistrict, /* EIM-49995 */
 adjustercode_alfa_stg

 FROM DB_T_PROD_STAG.cc_user cu

 INNER JOIN DB_T_PROD_STAG.cc_contact cc ON cc.id_stg=cu.ContactID_stg

 LEFT JOIN DB_T_PROD_STAG.cc_groupuser cgu on cgu.UserID_stg = cu.id_stg 

			LEFT JOIN DB_T_PROD_STAG.cc_group cg ON cg.id_stg=cgu.GroupID_stg

			LEFT JOIN DB_T_PROD_STAG.cctl_grouptype cct  ON cct.id_stg =cg.GroupType_stg

			WHERE cct.Name_stg in (''District'')

		)adjusterDistricts where  rankDistrict=1

 and adjustercode_alfa_stg is not null



union



/* Claim Hierarchy District/regional Manager */

SELECT

''PRTY_RLTD_ROLE25''  as Relationship, /*  Need to add value in XLAT table for PRTY_RLTD_ROLE */
cct.TYPECODE_stg,

cg.name_stg as PNI_AddressBookUID, /*  Can load value in this way as GRoup. */
cc.PublicID_stg AS OTHER_AddressBookUID,

cast(NULL as varchar(30)) AS Name1,

cg.createtime_stg as EffectiveDate,

cast(NULL as timestamp(6)) as ExpirationDate,

cc.UpdateTime_stg AS Updatetime,

''INTRNL_ORG Claim_INDIV'' as Lookups, 

 case when cu.retired_stg=0 and cg.retired_stg=0 and cc.retired_stg=0 and cct.retired_stg=0 then 0 else 1 end as Retired,

''PRTY_STRC_TYPE5'' AS Party_Structure_Type_Cd

from DB_T_PROD_STAG.cc_group cg 

 inner join DB_T_PROD_STAG.cc_groupuser cgu on cgu.groupid_stg = cg.id_stg

inner join DB_T_PROD_STAG.cctl_grouptype cct on cct.id_stg=cg.GroupType_stg

inner join DB_T_PROD_STAG.cc_user cu on cu.id_stg=cgu.UserID_stg and cgu.Manager_stg=1

inner join DB_T_PROD_STAG.cc_contact cc on cc.id_stg=cu.ContactID_stg

where cct.TYPECODE_stg in (''region_alfa'')



union



SELECT

''PRTY_RLTD_ROLE27''  as Relationship, /*  Need to add value in XLAT table for PRTY_RLTD_ROLE */
cct.TYPECODE_stg,

cg.name_stg as PNI_AddressBookUID, /*  Can load value in this way as GRoup. */
cc.PublicID_stg AS OTHER_AddressBookUID,

cast(NULL as varchar(30)) AS Name1,

cg.createtime_stg as EffectiveDate,

cast(NULL as timestamp(6)) as ExpirationDate,

cg.UpdateTime_stg AS Updatetime,

''INTRNL_ORG Claim_INDIV'' as Lookups, 

 case when cu.retired_stg=0 and cg.retired_stg=0 and cc.retired_stg=0 and cct.retired_stg=0 then 0 else 1 end as Retired,

''PRTY_STRC_TYPE5'' AS Party_Structure_Type_Cd

from DB_T_PROD_STAG.cc_group cg 

 inner join DB_T_PROD_STAG.cc_groupuser cgu on cgu.groupid_stg= cg.id_stg

inner join DB_T_PROD_STAG.cctl_grouptype cct on cct.id_stg=cg.GroupType_stg

inner join DB_T_PROD_STAG.cc_user cu on cu.id_stg=cgu.UserID_stg and cgu.Manager_stg=1

inner join DB_T_PROD_STAG.cc_contact cc on cc.id_stg=cu.ContactID_stg

where cct.TYPECODE_stg in (''district_alfa'')



union



/* Claim Hierarchy District/regional Supervisor */

SELECT

''PRTY_RLTD_ROLE28''  as Relationship, /*  Need to add value in XLAT table for PRTY_RLTD_ROLE */
  cct.TYPECODE_stg,

cg.name_stg as PNI_AddressBookUID, /*  Can load value in this way as GRoup. */
  cc.PublicID_stg AS OTHER_AddressBookUID,

  cast(NULL as varchar(30)) AS Name1,

  cg.createtime_stg as effectivedate,

  cast(NULL as timestamp(6)) as ExpirationDate,

  cg.UpdateTime_stg AS Updatetime,

  ''INTRNL_ORG Claim_INDIV'' as Lookups, 

  case when cu.retired_stg=0 and cg.retired_stg=0 and cc.retired_stg=0 and cct.retired_stg=0 then 0 else 1 end as Retired,

''PRTY_STRC_TYPE5'' AS Party_Structure_Type_Cd

from DB_T_PROD_STAG.cc_group cg 

 inner join DB_T_PROD_STAG.cctl_grouptype cct on cct.id_stg=cg.GroupType_stg

inner join DB_T_PROD_STAG.cc_user cu on cu.id_stg=cg.SupervisorID_stg

inner join DB_T_PROD_STAG.cc_contact cc on cc.id_stg=cu.ContactID_stg

where cct.TYPECODE_stg in (''district_alfa'')

/* order by TYPECODE,cc_group.Name,cc_group.UpdateTime */


union



SELECT

''PRTY_RLTD_ROLE25''  as Relationship, /*  Need to add value in XLAT table for PRTY_RLTD_ROLE */
  cct.TYPECODE_stg,

cg.name_stg as PNI_AddressBookUID, /*  Can load value in this way as GRoup. */
  cc.PublicID_stg AS OTHER_AddressBookUID,

  cast(NULL as varchar(30)) AS Name1,

  cg.createtime_stg as effectivedate,

  cast(NULL as timestamp(6)) as ExpirationDate,

  cg.UpdateTime_stg AS Updatetime,

  ''INTRNL_ORG Claim_INDIV'' as Lookups, 

  case when cu.retired_stg=0 and cg.retired_stg=0 and cc.retired_stg=0 and cct.retired_stg=0 then 0 else 1 end as Retired,

''PRTY_STRC_TYPE5'' AS Party_Structure_Type_Cd

from DB_T_PROD_STAG.cc_group cg 

 inner join DB_T_PROD_STAG.cctl_grouptype cct on cct.id_stg=cg.GroupType_stg

inner join DB_T_PROD_STAG.cc_user cu on cu.id_stg=cg.SupervisorID_stg

inner join DB_T_PROD_STAG.cc_contact cc on cc.id_stg=cu.ContactID_stg

where cct.TYPECODE_stg in (''region_alfa'')





/*********************************** Claims Hierarchy END***********************************/



union



/* Agent to Producer Area */	



-- in doc its 			pc_userproducercode  date not present****/												 */
SELECT DISTINCT																

cast(''PRTY_RLTD_ROLE10'' as varchar(50)) as Relationship, 															

pc.PublicID_stg,/* -- Party ID from Individual										 */
pp.Code_stg,/* --- Related Party ID from Internal Org						 */
cast(NULL as varchar(100)) AS Code,																

cast(''INTRNL_ORG_SBTYPE2'' as varchar(30)) AS Name1,																

CAST(''1990-01-01 00:00:00.000000'' AS timestamp(6)) AS eff_dt, /*  Need to follow up with Ahmed			 */
CAST(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS end_dt, /*  Need to follow up with Ahmed		 */
cast(current_timestamp as timestamp(6))  AS UpdateTime,																	

cast(''Claim_INDIV INTRNL_ORG'' as varchar(50)) as Lookups, 													

case when pp.retired_stg=0 and pc.retired_stg=0 and pcc.retired_stg=0 and pr.retired_stg=0 then 0 else 1 end as Retired,	

cast(''PRTY_STRC_TYPE16'' as varchar(50)) AS Structure 

from  DB_T_PROD_STAG.pc_producercode	pp															

left outer join DB_T_PROD_STAG.pc_userproducercode pup on  pp.id_stg = pup.producercodeid_stg														

left outer join DB_T_PROD_STAG.pc_user pu on  pup.UserID_stg = pu.id_stg						

left outer join DB_T_PROD_STAG.pc_userrole pur on pu.id_stg = pur.UserID_stg									

left outer join DB_T_PROD_STAG.pc_role UserRole on pur.RoleID_stg = UserRole.ID_stg						

left outer join DB_T_PROD_STAG.pc_contact pc on  pu.ContactID_stg = pc.id_stg								

left outer join DB_T_PROD_STAG.pctl_contact pcc on  pc.Subtype_stg = pcc.ID_stg							

left outer join DB_T_PROD_STAG.pc_producercoderole ppr on  pp.id_stg = ppr.ProducerCodeID_stg															

left outer join DB_T_PROD_STAG.pc_role pr on  ppr.RoleID_stg = pr.ID_stg							

left outer join DB_T_PROD_STAG.pc_role ProducerCodeRole on  ppr.RoleID_stg = ProducerCodeRole.ID_stg																

where pcc.name_stg=''User Contact''															

AND pr.name_stg=''Agent''																

-- AND UserRole.name_stg = ''Agent'' 	/*EIM - 36268 */															 */
UNION																

/****Party related (Primary Named Insured and Prior Carrier******/								

SELECT DISTINCT																

''PRTY_RLTD_ROLE3'' as Relationship, /*  Need to get confirmation from Keitra			 */
pc.AddressBookUID_stg, /* Party ID from Policy_INDIVidual							 */
ppa.TYPECODE_stg, /* Related party ID from Business,						 */
	cast(NULL as varchar(30)) AS Code, 													

	cast(NULL as varchar(30)) AS Name1,															

CAST(''1990-01-01 00:00:00.000000'' AS timestamp(6)) AS eff_dt, /*  Need to follow up with Ahmed			 */
CAST(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS end_dt,/*  Need to follow up with Ahmed		 */
pp.updatetime_stg AS UpdateTime,															

	''BUSN Policy_INDIV'' as Lookups, 															

case when pc.retired_stg=0 and ppa.retired_stg=0 and pcc.retired_stg=0 and pp.retired_stg=0 and pj.retired_stg=0 then 0 else 1 end as Retired,																

cast(NULL as varchar(50)) AS Structure																

FROM																

 	DB_T_PROD_STAG.pc_policycontactrole ppc 															

	INNER JOIN DB_T_PROD_STAG.pc_contact pc ON pc.id_stg=ppc.ContactDenorm_stg															

	INNER JOIN DB_T_PROD_STAG.pctl_policycontactrole ppcp ON ppcp.id_stg=ppc.Subtype_stg															

	INNER JOIN DB_T_PROD_STAG.pctl_contact pcc ON pcc.id_stg=pc.Subtype_stg	 														

	INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp ON pp.id_stg=ppc.BranchID_stg															

	INNER JOIN DB_T_PROD_STAG.pc_effectivedatedfields pe ON pp.id_stg=pe.BranchID_stg															

	INNER JOIN DB_T_PROD_STAG.pc_job pj ON pj.id_stg=pp.JobID_stg															

	INNER JOIN DB_T_PROD_STAG.pctl_job pcj ON pcj.id_stg=pj.Subtype_stg															

	INNER JOIN DB_T_PROD_STAG.pctl_policyperiodstatus pps ON pps.id_stg=pp.Status_stg															

	INNER JOIN DB_T_PROD_STAG.pctl_priorcarrier_alfa ppa ON ppa.id_stg=pe.PriorCarrier_alfa_stg															

WHERE 																

	pps.TYPECODE_stg=''Bound'' AND 															

																

	ppcp.TYPECODE_stg=''PolicyPriNamedInsured''AND															

pe.ExpirationDate_stg IS NULL /* AND															 */
/* pc_effectivedatedfields.UpdateTime > 						 */


  AND (  ( ppc.UpdateTime_stg > (:start_dttm) and ppc.UpdateTime_stg <= (:end_dttm))

 or ( pc.UpdateTime_stg > (:start_dttm) and pc.UpdateTime_stg <= (:end_dttm))

 or ( pp.UpdateTime_stg > (:start_dttm) and pp.UpdateTime_stg <= (:end_dttm))

 or ( pj.UpdateTime_stg > (:start_dttm) and pj.UpdateTime_stg<= (:end_dttm))

 or ( pe.UpdateTime_stg > (:start_dttm) and pe.UpdateTime_stg <= (:end_dttm))  )									

union																											

/***********************************Sales Hierarchy Start***********************************/

/** Sales Hierarchy ALFA to State **/



-- in doc its 			pc_parentgroup/*  date not present****/	 */


/* Use INT ORG to get parent and child PARTY ID */
SELECT DISTINCT

''PRTY_RLTD_ROLE29'' as Relationship, /*  Need to add value in XLAT table */
pcg.TYPECODE_stg AS Subtype,

pg.name_stg as code,

''INTRNL_ORG_SBTYPE4'' AS Name1,

prz.code_stg AS Subtype,

CAST(''1990-01-01 00:00:00.000000'' AS timestamp(6)) AS eff_dt, /*  Need to follow up with Ahmed			 */
CAST(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS end_dt, /*  Need to follow up with Ahmed		 */
cast(current_timestamp as timestamp(6))  AS UpdateTime,		

''INTRNL_ORG'' as Lookups, 

 case when pg.retired_stg=0 and child.retired_stg=0 and district.retired_stg=0 then 0 else 1 end as Retired,

''PRTY_STRC_TYPE16'' AS Structure  /* SLSHRCHY */
FROM

/**************ALFA to MARKETING************/

DB_T_PROD_STAG.pc_parentgroup ppg INNER JOIN DB_T_PROD_STAG.pc_group pg on pg.id_stg=ppg.ForeignEntityID_stg

INNER JOIN DB_T_PROD_STAG.pc_group child on child.id_stg=ppg.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pctl_grouptype pcg on pcg.id_stg=pg.GroupType_stg

INNER JOIN DB_T_PROD_STAG.pctl_grouptype childgrouptype on childgrouptype.id_stg=child.GroupType_stg

/************Marketing to District************/

INNER JOIN DB_T_PROD_STAG.pc_parentgroup pg_marketing_district on pg_marketing_district.ForeignEntityID_stg=ppg.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pc_group district on district.id_stg=pg_marketing_district.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pctl_grouptype district_type on district_type.id_stg=district.GroupType_stg

/********District to State*****************/

INNER JOIN DB_T_PROD_STAG.pc_groupregion pgr on pgr.GroupID_stg=pg_marketing_district.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pc_region_zone prz on pgr.RegionID_stg=prz.RegionId_stg

where 

(pcg.TYPECODE_stg=''root'' and childgrouptype.TYPECODE_stg=''region'') 



UNION



/** Sales Hierarchy State to Marketing **/

/* Use INT ORG to get parent and child PARTY ID */
SELECT DISTINCT

''PRTY_RLTD_ROLE22'' as Relationship, /*  Need to add value in XLAT table */
  ''INTRNL_ORG_SBTYPE4'' AS Name1,

  prz.code_stg,

childgrouptype.TYPECODE_stg,

child.name_stg AS OTHER_AddressBookUID,

CAST(''1990-01-01 00:00:00.000000'' AS timestamp(6)) AS eff_dt, /*  Need to follow up with Ahmed			 */
CAST(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS end_dt, /*  Need to follow up with Ahmed		 */
cast(current_timestamp as timestamp(6))  AS UpdateTime,		

''INTRNL_ORG'' as Lookups, 

 case when pg.retired_stg=0 and child.retired_stg=0 and district.retired_stg=0 then 0 else 1 end as Retired,

''PRTY_STRC_TYPE16'' AS Structure  /* SLSHRCHY */
FROM

/**************ALFA to MARKETING************/

DB_T_PROD_STAG.pc_parentgroup ppg INNER JOIN DB_T_PROD_STAG.pc_group pg on pg.id_stg=ppg.ForeignEntityID_stg

INNER JOIN DB_T_PROD_STAG.pc_group child on child.id_stg=ppg.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pctl_grouptype pcg on pcg.id_stg=pg.GroupType_stg

INNER JOIN DB_T_PROD_STAG.pctl_grouptype childgrouptype on childgrouptype.id_stg=child.GroupType_stg

/************Marketing to District************/

INNER JOIN DB_T_PROD_STAG.pc_parentgroup pg_marketing_district on pg_marketing_district.ForeignEntityID_stg=ppg.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pc_group district on district.id_stg=pg_marketing_district.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pctl_grouptype district_type on district_type.id_stg=district.GroupType_stg

/********District to State*****************/

INNER JOIN DB_T_PROD_STAG.pc_groupregion pgr on pgr.GroupID_stg=pg_marketing_district.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pc_region_zone prz on pgr.RegionID_stg=prz.RegionId_stg

where 

(pcg.TYPECODE_stg=''root'' and childgrouptype.TYPECODE_stg=''region'') 

 union

/* All Supervisors*/

SELECT DISTINCT

case when childgrouptype.TYPECODE_stg=''custserv'' then ''PRTY_RLTD_ROLE53'' 

when childgrouptype.TYPECODE_stg=''homeofficeadmin'' then ''PRTY_RLTD_ROLE54''

when childgrouptype.TYPECODE_stg=''region'' then ''PRTY_RLTD_ROLE26''

when childgrouptype.TYPECODE_stg=''salesdistrict_alfa'' then ''PRTY_RLTD_ROLE28'' 

when childgrouptype.TYPECODE_stg=''servicecenter_alfa'' then ''PRTY_RLTD_ROLE31''

when childgrouptype.TYPECODE_stg=''underwritingdistrict_alfa'' then ''PRTY_RLTD_ROLE28''

when childgrouptype.TYPECODE_stg=''homeofficeuw'' then ''PRTY_RLTD_ROLE55''

else NULL end as Relationship,

childgrouptype.TYPECODE_stg,

child.name_stg AS PNI_AddressBookUID,

  pc.PublicID_stg AS OTHER_AddressBookUID,

  cast(NULL as varchar(30)) AS Name1,

CAST(''1990-01-01 00:00:00.000000'' AS timestamp(6)) AS eff_dt, /*  Need to follow up with Ahmed			 */
CAST(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS end_dt, /*  Need to follow up with Ahmed		 */
cast(current_timestamp as timestamp(6))  AS UpdateTime,		

''INTRNL_ORG Claim_INDIV'' as Lookups, 

 case when pg.retired_stg=0 and child.retired_stg=0 and pu.retired_stg=0 and pc.retired_stg=0 then 0 else 1 end as Retired,

case when childgrouptype.TYPECODE_stg in(''underwritingdistrict_alfa'',''homeofficeuw'') then ''PRTY_STRC_TYPE14''

else ''PRTY_STRC_TYPE17'' end as Structure  /* SLSPRSASSGN */
FROM

DB_T_PROD_STAG.pc_parentgroup ppg INNER JOIN DB_T_PROD_STAG.pc_group pg on pg.id_stg=ppg.ForeignEntityID_stg

INNER JOIN DB_T_PROD_STAG.pc_group child on child.id_stg=ppg.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pc_user pu on pu.id_stg=child.SupervisorID_stg

INNER JOIN DB_T_PROD_STAG.pc_contact pc on pc.id_stg=pu.ContactID_stg

INNER JOIN DB_T_PROD_STAG.pctl_grouptype pcg on pcg.id_stg=pg.GroupType_stg

INNER JOIN DB_T_PROD_STAG.pctl_grouptype childgrouptype on childgrouptype.id_stg=child.GroupType_stg

/* where  */
/* (pctl_grouptype.TYPECODE=''root'' and childgrouptype.TYPECODE=''region'')  */
union

/* All Managers*/

SELECT DISTINCT

case when childgrouptype.TYPECODE_stg=''custserv'' then ''PRTY_RLTD_ROLE51'' 

when childgrouptype.TYPECODE_stg=''homeofficeadmin'' then ''PRTY_RLTD_ROLE52''

when childgrouptype.TYPECODE_stg=''region'' then ''PRTY_RLTD_ROLE25''

when childgrouptype.TYPECODE_stg=''salesdistrict_alfa'' then ''PRTY_RLTD_ROLE27''

when childgrouptype.TYPECODE_stg=''servicecenter_alfa'' then ''PRTY_RLTD_ROLE32''

when childgrouptype.TYPECODE_stg=''underwritingdistrict_alfa'' then ''PRTY_RLTD_ROLE27''

when childgrouptype.TYPECODE_stg=''homeofficeuw'' then ''PRTY_RLTD_ROLE56''

else NULL end as Relationship,

/* ''RGNTOMGR'' as Relationship1, -- Need to add value in XLAT table */
childgrouptype.TYPECODE_stg,

child.name_stg AS PNI_AddressBookUID,

  pc.PublicID_stg AS OTHER_AddressBookUID,

  cast(NULL as varchar(30)) AS Name1,

CAST(''1990-01-01 00:00:00.000000'' AS timestamp(6)) AS eff_dt, /*  Need to follow up with Ahmed			 */
CAST(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS end_dt, /*  Need to follow up with Ahmed		 */
cast(current_timestamp as timestamp(6))  AS UpdateTime,	

''INTRNL_ORG Claim_INDIV'' as Lookups, 

 case when pg.retired_stg=0 and child.retired_stg=0 and pu.retired_stg=0 and pc.retired_stg=0 then 0 else 1 end as Retired,

case when childgrouptype.TYPECODE_stg in(''underwritingdistrict_alfa'',''homeofficeuw'') then ''PRTY_STRC_TYPE14''

else ''PRTY_STRC_TYPE17'' end as Structure  /* SLSPRSASSGN */
FROM

DB_T_PROD_STAG.pc_parentgroup ppg INNER JOIN DB_T_PROD_STAG.pc_group pg on pg.id_stg=ppg.ForeignEntityID_stg

INNER JOIN DB_T_PROD_STAG.pc_group child on child.id_stg=ppg.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pc_groupuser pgu on pgu.GroupID_stg=child.id_stg and pgu.Manager_stg=1

INNER JOIN DB_T_PROD_STAG.pc_user pu on pu.id_stg=pgu.UserID_stg 

 INNER JOIN DB_T_PROD_STAG.pc_contact pc on pc.id_stg=pu.ContactID_stg

INNER JOIN DB_T_PROD_STAG.pctl_grouptype pcg on pcg.id_stg=pg.GroupType_stg

INNER JOIN DB_T_PROD_STAG.pctl_grouptype childgrouptype on childgrouptype.id_stg=child.GroupType_stg



union



/* Region to District*/

SELECT DISTINCT

''PRTY_RLTD_ROLE23'' as Relationship, /*  Need to add value in XLAT table  */
 pcg.TYPECODE_stg,

pg.name_stg as PNI_AddressBookUID,

childgrouptype.TYPECODE_stg,

child.name_stg AS OTHER_AddressBookUID,

CAST(''1990-01-01 00:00:00.000000'' AS timestamp(6)) AS eff_dt, /*  Need to follow up with Ahmed			 */
CAST(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS end_dt, /*  Need to follow up with Ahmed		 */
cast(current_timestamp as timestamp(6))  AS UpdateTime,	

''INTRNL_ORG'' as Lookups, 

 case when pg.retired_stg=0 and child.retired_stg=0 then 0 else 1 end as Retired,

''PRTY_STRC_TYPE16'' AS Structure  /*  SLSHRCY */
FROM

DB_T_PROD_STAG.pc_parentgroup ppg INNER JOIN DB_T_PROD_STAG.pc_group pg on pg.id_stg=ppg.ForeignEntityID_stg

INNER JOIN DB_T_PROD_STAG.pc_group child on child.id_stg=ppg.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pctl_grouptype pcg on pcg.id_stg=pg.GroupType_stg

  INNER JOIN DB_T_PROD_STAG.pctl_grouptype childgrouptype on childgrouptype.id_stg=child.GroupType_stg

where 

(pcg.TYPECODE_stg=''region'' and childgrouptype.TYPECODE_stg=''salesdistrict_alfa'') 

/* OR (pctl_grouptype.TYPECODE=''salesdistrict_alfa'' and childgrouptype.TYPECODE=''servicecenter_alfa'') */
union



/*District to SVC*/



SELECT DISTINCT

''PRTY_RLTD_ROLE24'' as Relationship, /*  Need to add value in XLAT table  */
 pcg.TYPECODE_stg,

pg.name_stg as PNI_AddressBookUID,

childgrouptype.TYPECODE_stg,

child.name_stg AS OTHER_AddressBookUID,

CAST(''1990-01-01 00:00:00.000000'' AS timestamp(6)) AS eff_dt, /*  Need to follow up with Ahmed			 */
CAST(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS end_dt, /*  Need to follow up with Ahmed		 */
cast(current_timestamp as timestamp(6))  AS UpdateTime,

''INTRNL_ORG'' as Lookups, 

 case when pg.retired_stg=0 and child.retired_stg=0 then 0 else 1 end as Retired,

''PRTY_STRC_TYPE16'' AS Structure  /*  SLSHRCY */
FROM

DB_T_PROD_STAG.pc_parentgroup ppg INNER JOIN DB_T_PROD_STAG.pc_group pg on pg.id_stg=ppg.ForeignEntityID_stg

INNER JOIN DB_T_PROD_STAG.pc_group child on child.id_stg=ppg.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pctl_grouptype pcg on pcg.id_stg=pg.GroupType_stg

  INNER JOIN DB_T_PROD_STAG.pctl_grouptype childgrouptype on childgrouptype.id_stg=child.GroupType_stg

where 

/* (pctl_grouptype.TYPECODE=''region'' and childgrouptype.TYPECODE=''salesdistrict_alfa'') or */
(pcg.TYPECODE_stg=''salesdistrict_alfa'' and childgrouptype.TYPECODE_stg=''servicecenter_alfa'')

UNION



/*SELECT DISTINCT

pc_role.Name as Relationship,  Need to add value in XLAT table
''INTRNL_ORG_SBTYPE2'' AS Subtype,

pc_producercode.code AS PNI_AddressBookUID,

  pc_contact.PublicID AS OTHER_AddressBookUID,

  NULL AS Name,

convert(datetime, ''01/01/1990'', 101) AS eff_dt,  Need to follow up with Ahmed			
CAST(''9999-12-31 23:59:59.9999999'' AS datetime2(7)) AS end_dt,  Need to follow up with Ahmed		 
 getdate() AS UpdateTime,	

''INTRNL_ORG Policy_INDIV'' as Lookups, 

 case when pc_producercode.retired=0 and pc_user.retired=0 and pc_contact.retired=0 then 0 else 1 end as Retired,

''PRTY_STRC_TYPE17'' AS Structure   --SLSPRSASSGN 
,(:start_dttm) as start_dttm,

(:end_dttm) as end_dttm

from DB_T_PROD_STAG.pc_producercode INNER JOIN DB_T_PROD_STAG.pc_userproducercode on pc_userproducercode.ProducerCodeID=pc_producercode.id

inner join DB_T_PROD_STAG.pc_user on pc_user.id=pc_userproducercode.UserID 

 INNER JOIN DB_T_PROD_STAG.pc_contact on pc_contact.id=pc_user.ContactID

INNER JOIN DB_T_PROD_STAG.pc_userrole on pc_userrole.UserID=pc_user.ID

INNER JOIN DB_T_PROD_STAG.pc_role on pc_role.id=pc_userrole.RoleID

where pc_role.Name in (''Agent'',''CSR'')*/



/****Adding as fix for EIM-13666- Ankit- 5/30/2017*********************/

/** SVC to Role **/

SELECT DISTINCT

i.Name_stg as Relationship, /*  Need to add value in XLAT table */
b.typecode_stg AS Subtype,

a.NameDenorm_stg AS PNI_AddressBookUID,

/* a.NameDenorm_stg collate  SQL_Latin1_General_CP1_CI_AS AS PNI_AddressBookUID, */
  e.PublicID_stg AS OTHER_AddressBookUID,

  cast(NULL as varchar(30)) AS Name1,

CAST(''1990-01-01 00:00:00.000000'' AS timestamp(6)) AS eff_dt, /*  Need to follow up with Ahmed			 */
CAST(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS end_dt, /*  Need to follow up with Ahmed		 */
cast(current_timestamp as timestamp(6))  AS UpdateTime,

''INTRNL_ORG Policy_INDIV'' as Lookups, 

 case when a.retired_stg=0 and d.retired_stg=0 and e.retired_stg=0 then 0 else 1 end as Retired,

''PRTY_STRC_TYPE17'' AS Structure  /* --SLSPRSASSGN */
from DB_T_PROD_STAG.pc_group a

INNER JOIN DB_T_PROD_STAG.pctl_grouptype b on b.id_stg = a.GroupType_stg

INNER JOIN DB_T_PROD_STAG.pc_groupuser c on c.GroupID_stg = a.id_stg

INNER JOIN DB_T_PROD_STAG.pc_user d on d.id_stg = c.UserID_stg

INNER JOIN DB_T_PROD_STAG.pc_contact e on e.id_stg = d.ContactID_stg 

INNER JOIN DB_T_PROD_STAG.pc_userrole h on h.UserID_stg = d.id_stg

INNER JOIN DB_T_PROD_STAG.pc_role i on i.id_stg = h.RoleID_stg

where b.typecode_stg = ''servicecenter_alfa''

/* and i.name_stg <> ''CRC Team Leaders'' */

UNION



/** Sales Hierarchy Producer to User **/

SELECT DISTINCT

''PRTY_RLTD_ROLE91'' as Relationship, /*  Need to add value in XLAT table */
''INTRNL_ORG_SBTYPE2'' AS Subtype,

 pp.code_stg AS PNI_AddressBookUID,

  pc.PublicID_stg AS OTHER_AddressBookUID,

  cast(NULL as varchar(30)) AS Name1,

CAST(''1990-01-01 00:00:00.000000'' AS timestamp(6)) AS eff_dt, /*  Need to follow up with Ahmed			 */
CAST(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS end_dt, /*  Need to follow up with Ahmed		 */
cast(current_timestamp as timestamp(6))  AS UpdateTime,	

''INTRNL_ORG Policy_INDIV'' as Lookups, 

 case when pp.retired_stg=0 and pu.retired_stg=0 and pc.retired_stg=0 then 0 else 1 end as Retired,

''PRTY_STRC_TYPE17'' AS Structure /* --SLSPRSASSGN */
from DB_T_PROD_STAG.pc_producercode pp INNER JOIN DB_T_PROD_STAG.pc_userproducercode pup on pup.ProducerCodeID_stg=pp.id_stg

inner join  DB_T_PROD_STAG.pc_user pu on pu.id_stg=pup.UserID_stg

 INNER JOIN DB_T_PROD_STAG.pc_contact pc on pc.id_stg=pu.ContactID_stg



UNION



/* Producer Area to Service center */

SELECT Relationship,TYPECODE1,PNI_AddressBookUID,TYPECODE2,OTHER_AddressBookUID,eff_dt,end_dt,UpdateTime,Lookups,Retired,Structure from 

(

SELECT DISTINCT

''PRTY_RLTD_ROLE30'' AS Relationship, 

 ''INTRNL_ORG_SBTYPE2'' AS TYPECODE1,

pp.Code_stg AS PNI_AddressBookUID, /* --- Party ID from Internal Org */
pcg.TYPECODE_stg AS TYPECODE2,

pg.NameDenorm_stg AS OTHER_AddressBookUID,  /* --- Related Party ID from Internal Org    */
/* pc_group.NameDenorm_stg collate  SQL_Latin1_General_CP1_CI_AS AS OTHER_AddressBookUID, */
CAST(''1990-01-01 00:00:00.000000'' AS timestamp(6)) AS eff_dt, /*  Need to follow up with Ahmed           */
CAST(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS end_dt, /*  Need to follow up with Ahmed   */
 pg.UpdateTime_stg AS UpdateTime, 

 ''INTRNL_ORG'' as Lookups, 

 case when pp.retired_stg=0 then 0 else 1 end as Retired,  

''PRTY_STRC_TYPE16'' AS Structure,  /* --- SLSHRCY */
        ROW_NUMBER() OVER (PARTITION BY pp.Code_stg  ORDER BY pg.UpdateTime_stg DESC) AS RN 

from 

DB_T_PROD_STAG.pc_group pg 

 join DB_T_PROD_STAG.pc_groupuser pgu on pg.ID_stg=pgu.GroupID_stg

join DB_T_PROD_STAG.pc_user pu on pgu.UserID_stg=pu.ID_stg

join DB_T_PROD_STAG.pc_contact pc on pu.ContactID_stg=pc.ID_stg

join DB_T_PROD_STAG.pctl_grouptype pcg on pg.GroupType_stg=pcg.ID_stg

/*  join DB_T_PROD_STAG.pctl_usertype pcu on pcu.ID_stg=pu.UserType_stg EIM -35544*/
LEFT join DB_T_PROD_STAG.pc_userproducercode pup on pu.ID_stg=pup.UserID_stg

LEFT join DB_T_PROD_STAG.pc_producercode pp on pup.ProducerCodeID_stg=pp.ID_stg

left join DB_T_PROD_STAG.pc_producercoderole ppr on pp.id_stg = ppr.ProducerCodeID_stg  /*EIM -35544*/

left join DB_T_PROD_STAG.pc_role pr on pr.id_stg = ppr.roleid_stg  /*EIM -35544*/

where 

pcg.typecode_stg = ''servicecenter_alfa''

and pp.code_stg is not null

and pg.namedenorm_stg is not null

and pr.name_stg = ''Agent'' /*EIM - 35544*/

) Temp

WHERE Temp.RN = 1



/***********************************Sales Hierarchy END***********************************/



UNION



/*********************************** Underwriter Hierarchy Start***********************************/

/************UW Relationship between ALFA and UW Home Office*****/

SELECT DISTINCT

''PRTY_RLTD_ROLE33'' as Relationship, /*  Need to add value in XLAT table */
pcg.TYPECODE_stg,

pg.name_stg as PNI_AddressBookUID,

childgrouptype.typecode_stg,

child.name_stg AS OTHER_AddressBookUID,

CAST(''1990-01-01 00:00:00.000000'' AS timestamp(6)) AS eff_dt, /*  Need to follow up with Ahmed			 */
CAST(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS end_dt, /*  Need to follow up with Ahmed		 */
cast(current_timestamp as timestamp(6))  AS UpdateTime,

''INTRNL_ORG'' as Lookups, 

 case when pg.retired_stg=0 and child.retired_stg=0 then 0 else 1 end as Retired,

''PRTY_STRC_TYPE13'' AS Structure



FROM

DB_T_PROD_STAG.pc_parentgroup ppg INNER JOIN DB_T_PROD_STAG.pc_group pg on pg.id_stg=ppg.ForeignEntityID_stg

INNER JOIN DB_T_PROD_STAG.pc_group child on child.id_stg=ppg.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pctl_grouptype pcg on pcg.id_stg=pg.GroupType_stg

  INNER JOIN DB_T_PROD_STAG.pctl_grouptype childgrouptype on childgrouptype.id_stg=child.GroupType_stg

where pcg.TYPECODE_stg in (''root'')

and childgrouptype.TYPECODE_stg in (''homeofficeuw'')



UNION



/**UW Office to State***/

SELECT DISTINCT

''PRTY_RLTD_ROLE34'' as Relationship, /*  Need to add value in XLAT table */
childgrouptype.typecode_stg,

child.name_stg AS OTHER_AddressBookUID,

''INTRNL_ORG_SBTYPE5'' AS Name,

prz.Code_stg,

CAST(''1990-01-01 00:00:00.000000'' AS timestamp(6)) AS eff_dt, /*  Need to follow up with Ahmed			 */
CAST(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS end_dt, /*  Need to follow up with Ahmed		 */
cast(current_timestamp as timestamp(6))  AS UpdateTime,

''INTRNL_ORG'' as Lookups, 

 case when pg.retired_stg=0 and child.retired_stg=0 and district.retired_stg=0 and pr.retired_stg=0 then 0 else 1 end as Retired,

''PRTY_STRC_TYPE13'' AS Structure

FROM

/** ALFA to UW Home Office **/

pc_parentgroup ppg INNER JOIN DB_T_PROD_STAG.pc_group pg on pg.id_stg=ppg.ForeignEntityID_stg

INNER JOIN DB_T_PROD_STAG.pc_group child on child.id_stg=ppg.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pctl_grouptype pcg on pcg.id_stg=pg.GroupType_stg

  INNER JOIN DB_T_PROD_STAG.pctl_grouptype childgrouptype on childgrouptype.id_stg=child.GroupType_stg

/** UW Office to UW District **/  

INNER JOIN DB_T_PROD_STAG.pc_parentgroup ho_to_district on ho_to_district.ForeignEntityID_stg=ppg.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pc_group district on district.id_stg=ho_to_district.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pctl_grouptype districttype on districttype.id_stg=district.GroupType_stg

/** Distrit to Region***/

INNER JOIN DB_T_PROD_STAG.pc_groupregion pgr on pgr.GroupID_stg=ho_to_district.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pc_region pr on pr.id_stg=pgr.RegionID_stg

/**Region to State**/

INNER JOIN DB_T_PROD_STAG.pc_region_zone prz on prz.RegionId_stg=pgr.RegionID_stg

where pcg.TYPECODE_stg in (''root'')

and childgrouptype.TYPECODE_stg in (''homeofficeuw'')



UNION

/**UW State to District****/

SELECT DISTINCT

''PRTY_RLTD_ROLE35'' as Relationship, /*  Need to add value in XLAT table */
''INTRNL_ORG_SBTYPE5'' AS Name,

prz.Code_stg,

districttype.TYPECODE_stg,

district.name_stg,

CAST(''1990-01-01 00:00:00.000000'' AS timestamp(6)) AS eff_dt, /*  Need to follow up with Ahmed			 */
CAST(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS end_dt, /*  Need to follow up with Ahmed		 */
cast(current_timestamp as timestamp(6))  AS UpdateTime,	

''INTRNL_ORG'' as Lookups, 

 case when pg.retired_stg=0 and child.retired_stg=0 and district.retired_stg=0 and pr.retired_stg=0 then 0 else 1 end as Retired,

''PRTY_STRC_TYPE13'' AS Structure

FROM

/** ALFA to UW Home Office **/

pc_parentgroup ppg INNER JOIN DB_T_PROD_STAG.pc_group pg on pg.id_stg=ppg.ForeignEntityID_stg

INNER JOIN DB_T_PROD_STAG.pc_group child on child.id_stg=ppg.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pctl_grouptype pcg on pcg.id_stg=pg.GroupType_stg

  INNER JOIN DB_T_PROD_STAG.pctl_grouptype childgrouptype on childgrouptype.id_stg=child.GroupType_stg

/** UW Office to UW District **/  

INNER JOIN DB_T_PROD_STAG.pc_parentgroup ho_to_district on ho_to_district.ForeignEntityID_stg=ppg.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pc_group district on district.id_stg=ho_to_district.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pctl_grouptype districttype on districttype.id_stg=district.GroupType_stg

/** Distrit to Region***/

INNER JOIN DB_T_PROD_STAG.pc_groupregion pgr on pgr.GroupID_stg=ho_to_district.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pc_region pr on pr.id_stg=pgr.RegionID_stg

/**Region to State**/

INNER JOIN DB_T_PROD_STAG.pc_region_zone prz on prz.RegionId_stg=pgr.RegionID_stg

where pcg.TYPECODE_stg in (''root'',''homeofficeuw'')

and childgrouptype.TYPECODE_stg in (''homeofficeuw'')



UNION



/***************UW District to Underwriter Relationship************/

select DISTINCT

''PRTY_RLTD_ROLE36'' as Relationship, /*  Need to add value in XLAT table */
  pcg.TYPECODE_stg,

pg.Name_stg as PNI_AddressBookUID, /* UW District */
pc.PublicID_stg AS OTHER_AddressBookUID, /* Underwriter */
  cast(null as varchar(30)) AS Name1,

CAST(''1990-01-01 00:00:00.000000'' AS timestamp(6)) AS eff_dt, /*  Need to follow up with Ahmed			 */
CAST(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS end_dt, /*  Need to follow up with Ahmed		 */
cast(current_timestamp as timestamp(6))  AS UpdateTime,	

''INTRNL_ORG Claim_INDIV'' as Lookups, 

 case when pu.retired_stg=0 and pg.retired_stg=0 and pc.retired_stg=0 then 0 else 1 end as Retired,

''PRTY_STRC_TYPE14'' AS Structure

from DB_T_PROD_STAG.pc_groupuser pgu INNER JOIN DB_T_PROD_STAG.pc_group pg on pg.id_stg=pgu.GroupID_stg

INNER JOIN DB_T_PROD_STAG.pc_user pu on pu.id_stg=pgu.UserID_stg

INNER JOIN DB_T_PROD_STAG.pc_contact pc on pc.id_stg=pu.ContactID_stg

INNER JOIN DB_T_PROD_STAG.pctl_grouptype pcg on pcg.id_stg=pg.GroupType_stg

where pcg.TYPECODE_stg=''underwritingdistrict_alfa''

) as a WHERE 

	relationship NOT IN (''PRTY_RLTD_ROLE3'',''PRTY_RLTD_ROLE12'',''PRTY_RLTD_ROLE27'',''PRTY_RLTD_ROLE28'',''PRTY_RLTD_ROLE41'',''PRTY_RLTD_ROLE24'',''PRTY_RLTD_ROLE43'',

	''PRTY_RLTD_ROLE23'',''PRTY_RLTD_ROLE45'',''PRTY_RLTD_ROLE22'',''PRTY_RLTD_ROLE34'',''PRTY_RLTD_ROLE35'',''PRTY_RLTD_ROLE30'')

	QUALIFY ROW_NUMBER() OVER (PARTITION BY PNI_AddressBookUID,OTHER_AddressBookUID,Relationship,Code,Name1,Lookups ORDER BY updatetime DESC)=1

ORDER BY effectivedate ASC,updatetime ASC
) SRC
)
);


-- Component exp_all_source1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_all_source1 AS
(
SELECT
SQ_pc_prty_rltd.Relationship as TYPECODE,
DECODE ( TRUE , LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT */ IS NULL , SQ_pc_prty_rltd.Relationship , LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT */ ) as var_TYPECODE,
DECODE ( TRUE , var_TYPECODE IS NULL , ''UNK'' , var_TYPECODE ) as var1_TYPECODE,
LKP_3.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_INTRNL_ORG_SBTYPE */ as var_Code,
LKP_4.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_INTRNL_ORG_TYPE */ as INTRNL_ORG_TYPE_CD,
LKP_5.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_INTRNL_ORG_SBTYPE */ as INTRNL_ORG_SBTYPE_CD,
LKP_6.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_INTRNL_ORG_SBTYPE */ as var_Name,
LKP_7.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SRC_CD */ as v_SYS_SRC_CD,
DECODE ( TRUE , UPPER ( SQ_pc_prty_rltd.Lookups ) = ''BUSN POLICY_INDIV'' , DECODE ( TRUE , LKP_8.INDIV_PRTY_ID /* replaced lookup LKP_INDIV_CNT_MGR */ IS NOT NULL , LKP_9.INDIV_PRTY_ID /* replaced lookup LKP_INDIV_CNT_MGR */ , LKP_10.BUSN_PRTY_ID /* replaced lookup LKP_BUSN */ ) , UPPER ( SQ_pc_prty_rltd.Lookups ) = ''POLICY_INDIV'' , LKP_11.INDIV_PRTY_ID /* replaced lookup LKP_INDIV_CNT_MGR */ , UPPER ( SQ_pc_prty_rltd.Lookups ) = ''CLAIM_INDIV INTRNL_ORG'' , LKP_12.INDIV_PRTY_ID /* replaced lookup LKP_INDIV_CLM_CTR */ , UPPER ( SQ_pc_prty_rltd.Lookups ) = ''INTRNL_ORG'' , LKP_13.INTRNL_ORG_PRTY_ID /* replaced lookup LKP_INTRNL_ORG */ , UPPER ( SQ_pc_prty_rltd.Lookups ) = ''INTRNL_ORG POLICY_INDIV'' , LKP_14.INTRNL_ORG_PRTY_ID /* replaced lookup LKP_INTRNL_ORG */ , UPPER ( SQ_pc_prty_rltd.Lookups ) = ''INTRNL_ORG CLAIM_INDIV'' , LKP_15.INTRNL_ORG_PRTY_ID /* replaced lookup LKP_INTRNL_ORG */ ) as var_PRTY_ID,
CASE WHEN var_PRTY_ID IS NULL THEN 9999 ELSE var_PRTY_ID END as out_PRTY_ID,
DECODE ( TRUE , UPPER ( SQ_pc_prty_rltd.Lookups ) = ''BUSN POLICY_INDIV'' , LKP_16.BUSN_PRTY_ID /* replaced lookup LKP_BUSN */ , UPPER ( SQ_pc_prty_rltd.Lookups ) = ''POLICY_INDIV'' , LKP_17.INDIV_PRTY_ID /* replaced lookup LKP_INDIV_CNT_MGR */ , UPPER ( SQ_pc_prty_rltd.Lookups ) = ''INTRNL_ORG'' , LKP_18.INTRNL_ORG_PRTY_ID /* replaced lookup LKP_INTRNL_ORG */ , UPPER ( SQ_pc_prty_rltd.Lookups ) = ''CLAIM_INDIV INTRNL_ORG'' , LKP_19.INTRNL_ORG_PRTY_ID /* replaced lookup LKP_INTRNL_ORG */ , UPPER ( SQ_pc_prty_rltd.Lookups ) = ''INTRNL_ORG POLICY_INDIV'' , LKP_20.INDIV_PRTY_ID /* replaced lookup LKP_INDIV_CLM_CTR */ , UPPER ( SQ_pc_prty_rltd.Lookups ) = ''INTRNL_ORG CLAIM_INDIV'' , LKP_21.INDIV_PRTY_ID /* replaced lookup LKP_INDIV_CLM_CTR */ ) as var_RLTD_PRTY_ID,
CASE WHEN var_RLTD_PRTY_ID IS NULL THEN 9999 ELSE var_RLTD_PRTY_ID END as out_RLTD_PRTY_ID,
SQ_pc_prty_rltd.updatetime as in_PRTY_RLTD_STRT_DTTM,
SQ_pc_prty_rltd.ExpirationDate as PRTY_RLTD_END_DTTM,
DECODE ( TRUE , LKP_22.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_PRTY_STRC */ IS NULL , ''UNK'' , LKP_23.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_PRTY_STRC */ ) as PRTY_STRC_TYPE_CD,
:PRCS_ID as PRCS_ID,
CASE WHEN SQ_pc_prty_rltd.updatetime IS NULL THEN TO_TIMESTAMP(''01/01/1900'', ''MM/DD/YYYY'') ELSE SQ_pc_prty_rltd.updatetime END as out_Trans_strt_dttm,
SQ_pc_prty_rltd.EffectiveDate as EffectiveDate,
SQ_pc_prty_rltd.Retired as Retired,
SQ_pc_prty_rltd.source_record_id,
row_number() over (partition by SQ_pc_prty_rltd.source_record_id order by SQ_pc_prty_rltd.source_record_id) as RNK
FROM
SQ_pc_prty_rltd
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = SQ_pc_prty_rltd.Relationship
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = SQ_pc_prty_rltd.Relationship
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_INTRNL_ORG_SBTYPE LKP_3 ON LKP_3.SRC_IDNTFTN_VAL = SQ_pc_prty_rltd.Code
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_INTRNL_ORG_TYPE LKP_4 ON LKP_4.SRC_IDNTFTN_VAL = ''INTRNL_ORG_TYPE15''
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_INTRNL_ORG_SBTYPE LKP_5 ON LKP_5.SRC_IDNTFTN_VAL = SQ_pc_prty_rltd.PNI_AddressBookUID
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_INTRNL_ORG_SBTYPE LKP_6 ON LKP_6.SRC_IDNTFTN_VAL = SQ_pc_prty_rltd.Name
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_SRC_CD LKP_7 ON LKP_7.SRC_IDNTFTN_VAL = SQ_pc_prty_rltd.SYS_SRC_CD
LEFT JOIN LKP_INDIV_CNT_MGR LKP_8 ON LKP_8.NK_LINK_ID = SQ_pc_prty_rltd.PNI_AddressBookUID
LEFT JOIN LKP_INDIV_CNT_MGR LKP_9 ON LKP_9.NK_LINK_ID = SQ_pc_prty_rltd.PNI_AddressBookUID
LEFT JOIN LKP_BUSN LKP_10 ON LKP_10.BUSN_CTGY_CD = ''CO'' AND LKP_10.NK_BUSN_CD = SQ_pc_prty_rltd.PNI_AddressBookUID
LEFT JOIN LKP_INDIV_CNT_MGR LKP_11 ON LKP_11.NK_LINK_ID = SQ_pc_prty_rltd.PNI_AddressBookUID
LEFT JOIN LKP_INDIV_CLM_CTR LKP_12 ON LKP_12.NK_PUBLC_ID = SQ_pc_prty_rltd.PNI_AddressBookUID
LEFT JOIN LKP_INTRNL_ORG LKP_13 ON LKP_13.INTRNL_ORG_TYPE_CD = INTRNL_ORG_TYPE_CD AND LKP_13.INTRNL_ORG_SBTYPE_CD = INTRNL_ORG_SBTYPE_CD AND LKP_13.INTRNL_ORG_NUM = SQ_pc_prty_rltd.OTHER_AddressBookUID
LEFT JOIN LKP_INTRNL_ORG LKP_14 ON LKP_14.INTRNL_ORG_TYPE_CD = INTRNL_ORG_TYPE_CD AND LKP_14.INTRNL_ORG_SBTYPE_CD = INTRNL_ORG_SBTYPE_CD AND LKP_14.INTRNL_ORG_NUM = SQ_pc_prty_rltd.OTHER_AddressBookUID
LEFT JOIN LKP_INTRNL_ORG LKP_15 ON LKP_15.INTRNL_ORG_TYPE_CD = INTRNL_ORG_TYPE_CD AND LKP_15.INTRNL_ORG_SBTYPE_CD = INTRNL_ORG_SBTYPE_CD AND LKP_15.INTRNL_ORG_NUM = SQ_pc_prty_rltd.OTHER_AddressBookUID
LEFT JOIN LKP_BUSN LKP_16 ON LKP_16.BUSN_CTGY_CD = ''INSCAR'' AND LKP_16.NK_BUSN_CD = SQ_pc_prty_rltd.OTHER_AddressBookUID
LEFT JOIN LKP_INDIV_CNT_MGR LKP_17 ON LKP_17.NK_LINK_ID = SQ_pc_prty_rltd.OTHER_AddressBookUID
LEFT JOIN LKP_INTRNL_ORG LKP_18 ON LKP_18.INTRNL_ORG_TYPE_CD = ''INT'' AND LKP_18.INTRNL_ORG_SBTYPE_CD = UPPER ( var_Code ) AND LKP_18.INTRNL_ORG_NUM = UPPER ( SQ_pc_prty_rltd.Name )
LEFT JOIN LKP_INTRNL_ORG LKP_19 ON LKP_19.INTRNL_ORG_TYPE_CD = ''INT'' AND LKP_19.INTRNL_ORG_SBTYPE_CD = UPPER ( var_Name ) AND LKP_19.INTRNL_ORG_NUM = UPPER ( SQ_pc_prty_rltd.OTHER_AddressBookUID )
LEFT JOIN LKP_INDIV_CLM_CTR LKP_20 ON LKP_20.NK_PUBLC_ID = SQ_pc_prty_rltd.Code
LEFT JOIN LKP_INDIV_CLM_CTR LKP_21 ON LKP_21.NK_PUBLC_ID = SQ_pc_prty_rltd.Code
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_PRTY_STRC LKP_22 ON LKP_22.SRC_IDNTFTN_VAL = SQ_pc_prty_rltd.Party_Structure_Type_Cd
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_PRTY_STRC LKP_23 ON LKP_23.SRC_IDNTFTN_VAL = SQ_pc_prty_rltd.Party_Structure_Type_Cd
QUALIFY RNK = 1
);


-- Component LKP_TERADATA_ETL_REF_XLAT_RELATIONSHIP1, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_RELATIONSHIP1 AS
(
SELECT
LKP.TGT_IDNTFTN_VAL,
exp_all_source1.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_all_source1.source_record_id ORDER BY LKP.TGT_IDNTFTN_VAL desc,LKP.SRC_IDNTFTN_VAL desc) RNK
FROM
exp_all_source1
LEFT JOIN (
SELECT 
	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL
	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 
FROM 
	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT
WHERE 
	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM in ( ''PRTY_RLTD_ROLE'')
             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM in  (''derived'',''pc_role.name'') 
		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS in (''DS'',''GW'') 
		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
) LKP ON LKP.SRC_IDNTFTN_VAL = exp_all_source1.TYPECODE
QUALIFY RNK = 1
);


-- Component exp_SrcFields1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_SrcFields1 AS
(
SELECT
exp_all_source1.out_PRTY_ID as in_PRTY_ID,
exp_all_source1.out_RLTD_PRTY_ID as in_RLTD_PRTY_ID,
LKP_TERADATA_ETL_REF_XLAT_RELATIONSHIP1.TGT_IDNTFTN_VAL as in_PRTY_RLTD_ROLE_CD,
exp_all_source1.in_PRTY_RLTD_STRT_DTTM as in_PRTY_RLTD_STRT_DTTM,
CASE WHEN exp_all_source1.PRTY_RLTD_END_DTTM IS NULL THEN TO_TIMESTAMP(''12/31/9999 23:59:59.999999'', ''MM/DD/YYYY HH24:MI:SS.NS'') ELSE exp_all_source1.PRTY_RLTD_END_DTTM END as o_PRTY_RLTD_END_DTTM,
exp_all_source1.PRTY_STRC_TYPE_CD as in_PRTY_STRC_TYPE_CD,
exp_all_source1.PRCS_ID as in_PRCS_ID,
exp_all_source1.out_Trans_strt_dttm as Trans_strt_dttm,
exp_all_source1.Retired as Retired,
CURRENT_TIMESTAMP as EDW_STRT_DTTM,
TO_TIMESTAMP(''12/31/9999 23:59:59.999999'', ''MM/DD/YYYY HH24:MI:SS.NS'') as EDW_END_DTTM,
DATEADD(''ss'', - 1, CURRENT_TIMESTAMP) as EDW_END_DTTM_exp,
exp_all_source1.EffectiveDate as EffectiveDate,
exp_all_source1.source_record_id
FROM
exp_all_source1
INNER JOIN LKP_TERADATA_ETL_REF_XLAT_RELATIONSHIP1 ON exp_all_source1.source_record_id = LKP_TERADATA_ETL_REF_XLAT_RELATIONSHIP1.source_record_id
);


-- Component LKP_PRTY_RLTD_CDC1, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_PRTY_RLTD_CDC1 AS
(
SELECT
LKP.PRTY_ID,
LKP.RLTD_PRTY_ID,
LKP.PRTY_RLTD_ROLE_CD,
LKP.PRTY_RLTD_STRT_DTTM,
LKP.PRTY_RLTD_END_DTTM,
LKP.PRTY_STRC_TYPE_CD,
LKP.EDW_STRT_DTTM,
LKP.EDW_END_DTTM,
LKP.TRANS_STRT_DTTM,
exp_SrcFields1.in_PRTY_ID as in_PRTY_ID,
exp_SrcFields1.in_RLTD_PRTY_ID as in_RLTD_PRTY_ID,
exp_SrcFields1.in_PRTY_RLTD_ROLE_CD as in_PRTY_RLTD_ROLE_CD,
exp_SrcFields1.o_PRTY_RLTD_END_DTTM as o_PRTY_RLTD_END_DTTM,
exp_SrcFields1.in_PRTY_STRC_TYPE_CD as in_PRTY_STRC_TYPE_CD,
exp_SrcFields1.in_PRCS_ID as in_PRCS_ID,
exp_SrcFields1.Trans_strt_dttm as Trans_strt_dttm1,
exp_SrcFields1.EDW_STRT_DTTM as EDW_STRT_DTTM1,
exp_SrcFields1.EDW_END_DTTM as EDW_END_DTTM1,
exp_SrcFields1.EDW_END_DTTM_exp as EDW_END_DTTM_exp,
exp_SrcFields1.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_SrcFields1.source_record_id ORDER BY LKP.PRTY_ID asc,LKP.RLTD_PRTY_ID asc,LKP.PRTY_RLTD_ROLE_CD asc,LKP.PRTY_RLTD_STRT_DTTM asc,LKP.PRTY_RLTD_END_DTTM asc,LKP.PRTY_STRC_TYPE_CD asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc,LKP.TRANS_STRT_DTTM asc) RNK
FROM
exp_SrcFields1
LEFT JOIN (
SELECT PRTY_RLTD.PRTY_RLTD_STRT_DTTM as PRTY_RLTD_STRT_DTTM, PRTY_RLTD.PRTY_RLTD_END_DTTM as PRTY_RLTD_END_DTTM, PRTY_RLTD.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
PRTY_RLTD.PRTY_STRC_TYPE_CD as PRTY_STRC_TYPE_CD, 
 PRTY_RLTD.EDW_STRT_DTTM as EDW_STRT_DTTM, PRTY_RLTD.EDW_END_DTTM as EDW_END_DTTM, PRTY_RLTD.PRTY_ID as PRTY_ID, PRTY_RLTD.RLTD_PRTY_ID as RLTD_PRTY_ID, PRTY_RLTD.PRTY_RLTD_ROLE_CD as PRTY_RLTD_ROLE_CD
  FROM DB_T_PROD_CORE.PRTY_RLTD WHERE PRTY_RLTD_ROLE_CD NOT IN (''PRIINSCAR'' ,''DISTADJSTR'',''DISTTOSPRVSR'',''DISTTOMGR'',''UWSTTODIST'',''RGNTODIST'',''STTORGN'',''DISTTOSVC'',''RGNTOGENL'',''DISTADJSTR'',''GENLTORGN'',''DISTTOSTRCEN'',''UWOFFCTOST'',''PRDASVC'')
QUALIFY ROW_NUMBER() OVER(PARTITION BY PRTY_ID,RLTD_PRTY_ID,PRTY_RLTD_ROLE_CD ORDER BY EDW_END_DTTM desc) = 1
) LKP ON LKP.PRTY_ID = exp_SrcFields1.in_PRTY_ID AND LKP.RLTD_PRTY_ID = exp_SrcFields1.in_RLTD_PRTY_ID AND LKP.PRTY_RLTD_ROLE_CD = exp_SrcFields1.in_PRTY_RLTD_ROLE_CD
QUALIFY RNK = 1
);


-- Component exp_CDC_Check1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_CDC_Check1 AS
(
SELECT
LKP_PRTY_RLTD_CDC1.in_PRTY_ID as in_PRTY_ID,
LKP_PRTY_RLTD_CDC1.in_RLTD_PRTY_ID as in_RLTD_PRTY_ID,
LKP_PRTY_RLTD_CDC1.in_PRTY_RLTD_ROLE_CD as in_PRTY_RLTD_ROLE_CD,
LKP_PRTY_RLTD_CDC1.o_PRTY_RLTD_END_DTTM as in_PRTY_RLTD_END_DTTM,
LKP_PRTY_RLTD_CDC1.in_PRTY_STRC_TYPE_CD as in_PRTY_STRC_TYPE_CD,
exp_SrcFields1.in_PRCS_ID as in_PRCS_ID,
LKP_PRTY_RLTD_CDC1.PRTY_ID as lkp_PRTY_ID,
LKP_PRTY_RLTD_CDC1.RLTD_PRTY_ID as lkp_RLTD_PRTY_ID,
LKP_PRTY_RLTD_CDC1.PRTY_RLTD_ROLE_CD as lkp_PRTY_RLTD_ROLE_CD,
LKP_PRTY_RLTD_CDC1.PRTY_RLTD_STRT_DTTM as lkp_PRTY_RLTD_STRT_DTTM,
LKP_PRTY_RLTD_CDC1.PRTY_RLTD_END_DTTM as lkp_PRTY_RLTD_END_DTTM,
LKP_PRTY_RLTD_CDC1.PRTY_STRC_TYPE_CD as lkp_PRTY_STRC_TYPE_CD,
NULL as lkp_PRCS_ID,
LKP_PRTY_RLTD_CDC1.EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
LKP_PRTY_RLTD_CDC1.EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_SrcFields1.EffectiveDate as EffectiveDate,
md5 ( to_char ( exp_SrcFields1.EffectiveDate ) || to_char ( LKP_PRTY_RLTD_CDC1.o_PRTY_RLTD_END_DTTM ) || ltrim ( rtrim ( LKP_PRTY_RLTD_CDC1.in_PRTY_STRC_TYPE_CD ) ) ) as var_calc_chksm,
md5 ( ltrim ( rtrim ( to_char ( LKP_PRTY_RLTD_CDC1.PRTY_RLTD_STRT_DTTM ) ) ) || ltrim ( rtrim ( to_char ( LKP_PRTY_RLTD_CDC1.PRTY_RLTD_END_DTTM ) ) ) || ltrim ( rtrim ( LKP_PRTY_RLTD_CDC1.PRTY_STRC_TYPE_CD ) ) ) as var_orig_chksm,
CASE WHEN var_orig_chksm IS NULL THEN ''I'' ELSE CASE WHEN var_orig_chksm != var_calc_chksm THEN ''U'' ELSE ''R'' END END as o_flag_MD5,
LKP_PRTY_RLTD_CDC1.EDW_STRT_DTTM1 as StartTime,
LKP_PRTY_RLTD_CDC1.EDW_END_DTTM1 as EndTime,
LKP_PRTY_RLTD_CDC1.Trans_strt_dttm1 as Trans_strt_dttm,
LKP_PRTY_RLTD_CDC1.EDW_END_DTTM_exp as EDW_END_DTTM_exp,
exp_SrcFields1.Retired as Retired,
exp_SrcFields1.source_record_id
FROM
exp_SrcFields1
INNER JOIN LKP_PRTY_RLTD_CDC1 ON exp_SrcFields1.source_record_id = LKP_PRTY_RLTD_CDC1.source_record_id
);


-- Component rtr_CDC1_Insert, Type ROUTER Output Group Insert
CREATE OR REPLACE TEMPORARY TABLE rtr_CDC1_Insert AS (
SELECT
exp_CDC_Check1.in_PRTY_ID as in_PRTY_ID,
exp_CDC_Check1.in_RLTD_PRTY_ID as in_RLTD_PRTY_ID,
exp_CDC_Check1.in_PRTY_RLTD_ROLE_CD as in_PRTY_RLTD_ROLE_CD,
NULL as in_PRTY_RLTD_STRT_DTTM,
exp_CDC_Check1.in_PRTY_RLTD_END_DTTM as in_PRTY_RLTD_END_DTTM,
exp_CDC_Check1.in_PRTY_STRC_TYPE_CD as in_PRTY_STRC_TYPE_CD,
exp_CDC_Check1.in_PRCS_ID as in_PRCS_ID,
exp_CDC_Check1.lkp_PRTY_ID as lkp_PRTY_ID,
exp_CDC_Check1.lkp_RLTD_PRTY_ID as lkp_RLTD_PRTY_ID,
exp_CDC_Check1.lkp_PRTY_RLTD_ROLE_CD as lkp_PRTY_RLTD_ROLE_CD,
exp_CDC_Check1.lkp_PRTY_RLTD_STRT_DTTM as lkp_PRTY_RLTD_STRT_DTTM,
exp_CDC_Check1.lkp_PRTY_RLTD_END_DTTM as lkp_PRTY_RLTD_END_DTTM,
exp_CDC_Check1.lkp_PRTY_STRC_TYPE_CD as lkp_PRTY_STRC_TYPE_CD,
exp_CDC_Check1.lkp_PRCS_ID as lkp_PRCS_ID,
exp_CDC_Check1.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_CDC_Check1.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
NULL as o_Src_Tgt,
exp_CDC_Check1.StartTime as StartTime,
exp_CDC_Check1.EndTime as EndTime,
exp_CDC_Check1.Trans_strt_dttm as Trans_strt_dttm,
exp_CDC_Check1.EDW_END_DTTM_exp as EDW_END_DTTM_exp,
exp_CDC_Check1.Retired as Retired,
exp_CDC_Check1.o_flag_MD5 as o_flag_MD5,
exp_CDC_Check1.EffectiveDate as EffectiveDate,
exp_CDC_Check1.source_record_id
FROM
exp_CDC_Check1
WHERE CASE WHEN exp_CDC_Check1.o_flag_MD5 = ''I'' AND exp_CDC_Check1.in_PRTY_ID != 9999 AND exp_CDC_Check1.in_RLTD_PRTY_ID != 9999 THEN 1 ELSE CASE WHEN exp_CDC_Check1.Retired = 0 AND exp_CDC_Check1.lkp_EDW_END_DTTM != TO_TIMESTAMP(''12/31/9999 23:59:59.999999'', ''MM/DD/YYYY HH24:MI:SS.NS'') THEN 1 ELSE 0 END END - - o_Src_Tgt = ''I'' OR ( exp_CDC_Check1.Retired = 0 AND exp_CDC_Check1.lkp_EDW_END_DTTM != TO_TIMESTAMP(''12/31/9999 23:59:59.999999'', ''MM/DD/YYYY HH24:MI:SS.NS'') and exp_CDC_Check1.lkp_PRTY_ID IS NOT NULL and exp_CDC_Check1.lkp_RLTD_PRTY_ID IS NOT NULL )
);


-- Component rtr_CDC1_Retired, Type ROUTER Output Group Retired
CREATE OR REPLACE TEMPORARY TABLE rtr_CDC1_Retired AS (
SELECT
exp_CDC_Check1.in_PRTY_ID as in_PRTY_ID,
exp_CDC_Check1.in_RLTD_PRTY_ID as in_RLTD_PRTY_ID,
exp_CDC_Check1.in_PRTY_RLTD_ROLE_CD as in_PRTY_RLTD_ROLE_CD,
NULL as in_PRTY_RLTD_STRT_DTTM,
exp_CDC_Check1.in_PRTY_RLTD_END_DTTM as in_PRTY_RLTD_END_DTTM,
exp_CDC_Check1.in_PRTY_STRC_TYPE_CD as in_PRTY_STRC_TYPE_CD,
exp_CDC_Check1.in_PRCS_ID as in_PRCS_ID,
exp_CDC_Check1.lkp_PRTY_ID as lkp_PRTY_ID,
exp_CDC_Check1.lkp_RLTD_PRTY_ID as lkp_RLTD_PRTY_ID,
exp_CDC_Check1.lkp_PRTY_RLTD_ROLE_CD as lkp_PRTY_RLTD_ROLE_CD,
exp_CDC_Check1.lkp_PRTY_RLTD_STRT_DTTM as lkp_PRTY_RLTD_STRT_DTTM,
exp_CDC_Check1.lkp_PRTY_RLTD_END_DTTM as lkp_PRTY_RLTD_END_DTTM,
exp_CDC_Check1.lkp_PRTY_STRC_TYPE_CD as lkp_PRTY_STRC_TYPE_CD,
exp_CDC_Check1.lkp_PRCS_ID as lkp_PRCS_ID,
exp_CDC_Check1.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_CDC_Check1.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
NULL as o_Src_Tgt,
exp_CDC_Check1.StartTime as StartTime,
exp_CDC_Check1.EndTime as EndTime,
exp_CDC_Check1.Trans_strt_dttm as Trans_strt_dttm,
exp_CDC_Check1.EDW_END_DTTM_exp as EDW_END_DTTM_exp,
exp_CDC_Check1.Retired as Retired,
exp_CDC_Check1.o_flag_MD5 as o_flag_MD5,
exp_CDC_Check1.EffectiveDate as EffectiveDate,
exp_CDC_Check1.source_record_id
FROM
exp_CDC_Check1
WHERE exp_CDC_Check1.o_flag_MD5 = ''R'' and exp_CDC_Check1.Retired != 0 and exp_CDC_Check1.lkp_EDW_END_DTTM = TO_TIMESTAMP(''12/31/9999 23:59:59.999999'', ''MM/DD/YYYY HH24:MI:SS.NS'') - - o_Src_Tgt = ''R'' and exp_CDC_Check1.Retired != 0 and exp_CDC_Check1.lkp_EDW_END_DTTM = TO_TIMESTAMP(''12/31/9999 23:59:59.999999'', ''MM/DD/YYYY HH24:MI:SS.NS'')
);


-- Component rtr_CDC1_Update, Type ROUTER Output Group Update
CREATE OR REPLACE TEMPORARY TABLE rtr_CDC1_Update AS (
SELECT
exp_CDC_Check1.in_PRTY_ID as in_PRTY_ID,
exp_CDC_Check1.in_RLTD_PRTY_ID as in_RLTD_PRTY_ID,
exp_CDC_Check1.in_PRTY_RLTD_ROLE_CD as in_PRTY_RLTD_ROLE_CD,
NULL as in_PRTY_RLTD_STRT_DTTM,
exp_CDC_Check1.in_PRTY_RLTD_END_DTTM as in_PRTY_RLTD_END_DTTM,
exp_CDC_Check1.in_PRTY_STRC_TYPE_CD as in_PRTY_STRC_TYPE_CD,
exp_CDC_Check1.in_PRCS_ID as in_PRCS_ID,
exp_CDC_Check1.lkp_PRTY_ID as lkp_PRTY_ID,
exp_CDC_Check1.lkp_RLTD_PRTY_ID as lkp_RLTD_PRTY_ID,
exp_CDC_Check1.lkp_PRTY_RLTD_ROLE_CD as lkp_PRTY_RLTD_ROLE_CD,
exp_CDC_Check1.lkp_PRTY_RLTD_STRT_DTTM as lkp_PRTY_RLTD_STRT_DTTM,
exp_CDC_Check1.lkp_PRTY_RLTD_END_DTTM as lkp_PRTY_RLTD_END_DTTM,
exp_CDC_Check1.lkp_PRTY_STRC_TYPE_CD as lkp_PRTY_STRC_TYPE_CD,
exp_CDC_Check1.lkp_PRCS_ID as lkp_PRCS_ID,
exp_CDC_Check1.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_CDC_Check1.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
NULL as o_Src_Tgt,
exp_CDC_Check1.StartTime as StartTime,
exp_CDC_Check1.EndTime as EndTime,
exp_CDC_Check1.Trans_strt_dttm as Trans_strt_dttm,
exp_CDC_Check1.EDW_END_DTTM_exp as EDW_END_DTTM_exp,
exp_CDC_Check1.Retired as Retired,
exp_CDC_Check1.o_flag_MD5 as o_flag_MD5,
exp_CDC_Check1.EffectiveDate as EffectiveDate,
exp_CDC_Check1.source_record_id
FROM
exp_CDC_Check1
WHERE exp_CDC_Check1.o_flag_MD5 = ''U'' AND exp_CDC_Check1.lkp_EDW_END_DTTM = TO_TIMESTAMP(''12/31/9999 23:59:59.999999'', ''MM/DD/YYYY HH24:MI:SS.NS'') - - o_Src_Tgt = ''U'' AND exp_CDC_Check1.lkp_EDW_END_DTTM = TO_TIMESTAMP(''12/31/9999 23:59:59.999999'', ''MM/DD/YYYY HH24:MI:SS.NS'') and exp_CDC_Check1.lkp_PRTY_ID IS NOT NULL and exp_CDC_Check1.lkp_RLTD_PRTY_ID IS NOT NULL
);


-- Component upd_insert1, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_insert1 AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_CDC1_Insert.in_PRTY_ID as in_PRTY_ID1,
rtr_CDC1_Insert.in_RLTD_PRTY_ID as in_RLTD_PRTY_ID1,
rtr_CDC1_Insert.in_PRTY_RLTD_ROLE_CD as in_PRTY_RLTD_ROLE_CD1,
rtr_CDC1_Insert.in_PRTY_RLTD_STRT_DTTM as in_PRTY_RLTD_STRT_DTTM1,
rtr_CDC1_Insert.in_PRTY_RLTD_END_DTTM as in_PRTY_RLTD_END_DTTM1,
rtr_CDC1_Insert.in_PRTY_STRC_TYPE_CD as in_PRTY_STRC_TYPE_CD1,
rtr_CDC1_Insert.in_PRCS_ID as in_PRCS_ID1,
rtr_CDC1_Insert.StartTime as StartTime1,
rtr_CDC1_Insert.EndTime as EndTime1,
rtr_CDC1_Insert.Trans_strt_dttm as Trans_strt_dttm1,
rtr_CDC1_Insert.Retired as Retired1,
rtr_CDC1_Insert.EffectiveDate as EffectiveDate1,
rtr_CDC1_Insert.source_record_id,
0 as UPDATE_STRATEGY_ACTION
FROM
rtr_CDC1_Insert
);


-- Component upd_Upd_retired1, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_Upd_retired1 AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_CDC1_Retired.lkp_PRTY_ID as lkp_PRTY_ID3,
rtr_CDC1_Retired.lkp_RLTD_PRTY_ID as lkp_RLTD_PRTY_ID3,
rtr_CDC1_Retired.lkp_PRTY_RLTD_ROLE_CD as lkp_PRTY_RLTD_ROLE_CD3,
rtr_CDC1_Retired.lkp_PRTY_RLTD_STRT_DTTM as lkp_PRTY_RLTD_STRT_DTTM3,
rtr_CDC1_Retired.lkp_PRTY_RLTD_END_DTTM as lkp_PRTY_RLTD_END_DTTM3,
NULL as lkp_PRTY_STRC_TYPE_CD3,
rtr_CDC1_Retired.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM3,
NULL as EDW_END_DTTM_exp3,
rtr_CDC1_Retired.Trans_strt_dttm as Trans_strt_dttm4,
rtr_CDC1_Retired.source_record_id,
1 as UPDATE_STRATEGY_ACTION
FROM
rtr_CDC1_Retired
);


-- Component exp_prty_rltd_insert1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_prty_rltd_insert1 AS
(
SELECT
upd_insert1.in_PRTY_ID1 as in_PRTY_ID1,
upd_insert1.in_RLTD_PRTY_ID1 as in_RLTD_PRTY_ID1,
upd_insert1.in_PRTY_RLTD_ROLE_CD1 as in_PRTY_RLTD_ROLE_CD1,
upd_insert1.in_PRTY_RLTD_END_DTTM1 as in_PRTY_RLTD_END_DTTM1,
upd_insert1.in_PRTY_STRC_TYPE_CD1 as in_PRTY_STRC_TYPE_CD1,
upd_insert1.in_PRCS_ID1 as in_PRCS_ID1,
upd_insert1.StartTime1 as StartTime1,
upd_insert1.Trans_strt_dttm1 as Trans_strt_dttm1,
CASE WHEN upd_insert1.Retired1 = 0 THEN TO_TIMESTAMP(''9999-12-31 23:59:59.999999'', ''YYYY-MM-DD HH24:MI:SS.NS'') ELSE CURRENT_TIMESTAMP END as out_EDW_END_DTTM,
CASE WHEN upd_insert1.Retired1 != 0 THEN upd_insert1.Trans_strt_dttm1 ELSE TO_TIMESTAMP(''9999-12-31 23:59:59.999999'', ''YYYY-MM-DD HH24:MI:SS.NS'') END as TRANS_END_DTTM,
upd_insert1.EffectiveDate1 as EffectiveDate1,
upd_insert1.source_record_id
FROM
upd_insert1
);


-- Component upd_Update1, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_Update1 AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_CDC1_Update.lkp_PRTY_ID as lkp_PRTY_ID3,
rtr_CDC1_Update.lkp_RLTD_PRTY_ID as lkp_RLTD_PRTY_ID3,
rtr_CDC1_Update.lkp_PRTY_RLTD_ROLE_CD as lkp_PRTY_RLTD_ROLE_CD3,
rtr_CDC1_Update.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM3,
rtr_CDC1_Update.EDW_END_DTTM_exp as EDW_END_DTTM_exp3,
rtr_CDC1_Update.lkp_PRTY_RLTD_STRT_DTTM as lkp_PRTY_RLTD_STRT_DTTM3,
rtr_CDC1_Update.lkp_PRTY_RLTD_END_DTTM as lkp_PRTY_RLTD_END_DTTM3,
rtr_CDC1_Update.lkp_PRTY_STRC_TYPE_CD as lkp_PRTY_STRC_TYPE_CD3,
rtr_CDC1_Update.Trans_strt_dttm as Trans_strt_dttm3,
rtr_CDC1_Update.source_record_id,
1 as UPDATE_STRATEGY_ACTION
FROM
rtr_CDC1_Update
);


-- Component EXPTRANS_upd_retired1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXPTRANS_upd_retired1 AS
(
SELECT
upd_Upd_retired1.lkp_PRTY_ID3 as lkp_PRTY_ID3,
upd_Upd_retired1.lkp_RLTD_PRTY_ID3 as lkp_RLTD_PRTY_ID3,
upd_Upd_retired1.lkp_PRTY_RLTD_ROLE_CD3 as lkp_PRTY_RLTD_ROLE_CD3,
upd_Upd_retired1.lkp_EDW_STRT_DTTM3 as lkp_EDW_STRT_DTTM3,
CURRENT_TIMESTAMP as out_EDW_END_DTTM,
upd_Upd_retired1.Trans_strt_dttm4 as Trans_strt_dttm4,
upd_Upd_retired1.source_record_id
FROM
upd_Upd_retired1
);


-- Component upd_Upd_ins1, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_Upd_ins1 AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_CDC1_Update.in_PRTY_ID as PRTY_ID3,
rtr_CDC1_Update.in_RLTD_PRTY_ID as RLTD_PRTY_ID3,
rtr_CDC1_Update.in_PRTY_RLTD_ROLE_CD as PRTY_RLTD_ROLE_CD3,
rtr_CDC1_Update.in_PRTY_RLTD_STRT_DTTM as in_PRTY_RLTD_STRT_DTTM3,
NULL as lkp_PRTY_RLTD_STRT_DTTM3,
NULL as lkp_PRTY_RLTD_END_DTTM3,
NULL as lkp_PRTY_STRC_TYPE_CD3,
rtr_CDC1_Update.in_PRTY_STRC_TYPE_CD as in_PRTY_STRC_TYPE_CD3,
rtr_CDC1_Update.in_PRTY_RLTD_END_DTTM as in_PRTY_RLTD_END_DTTM3,
rtr_CDC1_Update.in_PRCS_ID as in_PRCS_ID3,
rtr_CDC1_Update.StartTime as StartTime3,
rtr_CDC1_Update.EndTime as EndTime3,
rtr_CDC1_Update.Trans_strt_dttm as Trans_strt_dttm3,
rtr_CDC1_Update.Retired as Retired3,
rtr_CDC1_Update.EffectiveDate as EffectiveDate3,
rtr_CDC1_Update.source_record_id,
0 as UPDATE_STRATEGY_ACTION
FROM
rtr_CDC1_Update
);


-- Component tgt_prty_rltd_Upd_retired, Type TARGET 
MERGE INTO DB_T_PROD_CORE.PRTY_RLTD
USING EXPTRANS_upd_retired1 ON (PRTY_RLTD.PRTY_ID = EXPTRANS_upd_retired1.lkp_PRTY_ID3 AND PRTY_RLTD.RLTD_PRTY_ID = EXPTRANS_upd_retired1.lkp_RLTD_PRTY_ID3 AND PRTY_RLTD.PRTY_RLTD_ROLE_CD = EXPTRANS_upd_retired1.lkp_PRTY_RLTD_ROLE_CD3 AND PRTY_RLTD.EDW_STRT_DTTM = EXPTRANS_upd_retired1.lkp_EDW_STRT_DTTM3)
WHEN MATCHED THEN UPDATE
SET
PRTY_ID = EXPTRANS_upd_retired1.lkp_PRTY_ID3,
RLTD_PRTY_ID = EXPTRANS_upd_retired1.lkp_RLTD_PRTY_ID3,
PRTY_RLTD_ROLE_CD = EXPTRANS_upd_retired1.lkp_PRTY_RLTD_ROLE_CD3,
EDW_STRT_DTTM = EXPTRANS_upd_retired1.lkp_EDW_STRT_DTTM3,
EDW_END_DTTM = EXPTRANS_upd_retired1.out_EDW_END_DTTM,
TRANS_END_DTTM = EXPTRANS_upd_retired1.Trans_strt_dttm4;


-- Component tgt_prty_rltd_NewInsert1, Type TARGET 
INSERT INTO DB_T_PROD_CORE.PRTY_RLTD
(
PRTY_ID,
RLTD_PRTY_ID,
PRTY_RLTD_ROLE_CD,
PRTY_RLTD_STRT_DTTM,
PRTY_RLTD_END_DTTM,
PRTY_STRC_TYPE_CD,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM,
TRANS_END_DTTM
)
SELECT
exp_prty_rltd_insert1.in_PRTY_ID1 as PRTY_ID,
exp_prty_rltd_insert1.in_RLTD_PRTY_ID1 as RLTD_PRTY_ID,
exp_prty_rltd_insert1.in_PRTY_RLTD_ROLE_CD1 as PRTY_RLTD_ROLE_CD,
exp_prty_rltd_insert1.EffectiveDate1 as PRTY_RLTD_STRT_DTTM,
exp_prty_rltd_insert1.in_PRTY_RLTD_END_DTTM1 as PRTY_RLTD_END_DTTM,
exp_prty_rltd_insert1.in_PRTY_STRC_TYPE_CD1 as PRTY_STRC_TYPE_CD,
exp_prty_rltd_insert1.in_PRCS_ID1 as PRCS_ID,
exp_prty_rltd_insert1.StartTime1 as EDW_STRT_DTTM,
exp_prty_rltd_insert1.out_EDW_END_DTTM as EDW_END_DTTM,
exp_prty_rltd_insert1.Trans_strt_dttm1 as TRANS_STRT_DTTM,
exp_prty_rltd_insert1.TRANS_END_DTTM as TRANS_END_DTTM
FROM
exp_prty_rltd_insert1;


-- Component EXPTRANS1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXPTRANS1 AS
(
SELECT
upd_Update1.lkp_PRTY_ID3 as lkp_PRTY_ID3,
upd_Update1.lkp_RLTD_PRTY_ID3 as lkp_RLTD_PRTY_ID3,
upd_Update1.lkp_PRTY_RLTD_ROLE_CD3 as lkp_PRTY_RLTD_ROLE_CD3,
upd_Update1.lkp_EDW_STRT_DTTM3 as lkp_EDW_STRT_DTTM3,
upd_Update1.EDW_END_DTTM_exp3 as Expiring_EndDate,
DATEADD(SECOND, -1, upd_Update1.Trans_strt_dttm3) as Trans_strt_dttm31,
upd_Update1.source_record_id
FROM
upd_Update1
);


-- Component FILTRANS1, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE FILTRANS1 AS
(
SELECT
upd_Upd_ins1.PRTY_ID3 as PRTY_ID3,
upd_Upd_ins1.RLTD_PRTY_ID3 as RLTD_PRTY_ID3,
upd_Upd_ins1.PRTY_RLTD_ROLE_CD3 as PRTY_RLTD_ROLE_CD3,
upd_Upd_ins1.in_PRTY_STRC_TYPE_CD3 as in_PRTY_STRC_TYPE_CD3,
upd_Upd_ins1.in_PRTY_RLTD_STRT_DTTM3 as in_PRTY_RLTD_STRT_DTTM3,
upd_Upd_ins1.in_PRTY_RLTD_END_DTTM3 as in_PRTY_RLTD_END_DTTM3,
upd_Upd_ins1.in_PRCS_ID3 as in_PRCS_ID3,
upd_Upd_ins1.StartTime3 as StartTime3,
upd_Upd_ins1.EndTime3 as EndTime3,
upd_Upd_ins1.Trans_strt_dttm3 as Trans_strt_dttm3,
upd_Upd_ins1.Retired3 as Retired3,
upd_Upd_ins1.EffectiveDate3 as EffectiveDate3,
upd_Upd_ins1.source_record_id
FROM
upd_Upd_ins1
WHERE upd_Upd_ins1.Retired3 = 0
);


-- Component EXPTRANS_UPD_INS1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXPTRANS_UPD_INS1 AS
(
SELECT
FILTRANS1.PRTY_ID3 as PRTY_ID3,
FILTRANS1.RLTD_PRTY_ID3 as RLTD_PRTY_ID3,
FILTRANS1.PRTY_RLTD_ROLE_CD3 as PRTY_RLTD_ROLE_CD3,
FILTRANS1.in_PRTY_STRC_TYPE_CD3 as in_PRTY_STRC_TYPE_CD3,
FILTRANS1.in_PRTY_RLTD_END_DTTM3 as in_PRTY_RLTD_END_DTTM3,
FILTRANS1.in_PRCS_ID3 as in_PRCS_ID3,
FILTRANS1.StartTime3 as StartTime3,
FILTRANS1.EndTime3 as EndTime3,
FILTRANS1.Trans_strt_dttm3 as Trans_strt_dttm3,
FILTRANS1.EffectiveDate3 as EffectiveDate3,
FILTRANS1.source_record_id
FROM
FILTRANS1
);


-- Component tgt_prty_rltd_Upd_Insert, Type TARGET 
INSERT INTO DB_T_PROD_CORE.PRTY_RLTD
(
PRTY_ID,
RLTD_PRTY_ID,
PRTY_RLTD_ROLE_CD,
PRTY_RLTD_STRT_DTTM,
PRTY_RLTD_END_DTTM,
PRTY_STRC_TYPE_CD,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM
)
SELECT
EXPTRANS_UPD_INS1.PRTY_ID3 as PRTY_ID,
EXPTRANS_UPD_INS1.RLTD_PRTY_ID3 as RLTD_PRTY_ID,
EXPTRANS_UPD_INS1.PRTY_RLTD_ROLE_CD3 as PRTY_RLTD_ROLE_CD,
EXPTRANS_UPD_INS1.EffectiveDate3 as PRTY_RLTD_STRT_DTTM,
EXPTRANS_UPD_INS1.in_PRTY_RLTD_END_DTTM3 as PRTY_RLTD_END_DTTM,
EXPTRANS_UPD_INS1.in_PRTY_STRC_TYPE_CD3 as PRTY_STRC_TYPE_CD,
EXPTRANS_UPD_INS1.in_PRCS_ID3 as PRCS_ID,
EXPTRANS_UPD_INS1.StartTime3 as EDW_STRT_DTTM,
EXPTRANS_UPD_INS1.EndTime3 as EDW_END_DTTM,
EXPTRANS_UPD_INS1.Trans_strt_dttm3 as TRANS_STRT_DTTM
FROM
EXPTRANS_UPD_INS1;


-- Component tgt_prty_rltd_Update, Type TARGET 
MERGE INTO DB_T_PROD_CORE.PRTY_RLTD
USING EXPTRANS1 ON (PRTY_RLTD.PRTY_ID = EXPTRANS1.lkp_PRTY_ID3 AND PRTY_RLTD.RLTD_PRTY_ID = EXPTRANS1.lkp_RLTD_PRTY_ID3 AND PRTY_RLTD.PRTY_RLTD_ROLE_CD = EXPTRANS1.lkp_PRTY_RLTD_ROLE_CD3 AND PRTY_RLTD.EDW_STRT_DTTM = EXPTRANS1.lkp_EDW_STRT_DTTM3)
WHEN MATCHED THEN UPDATE
SET
PRTY_ID = EXPTRANS1.lkp_PRTY_ID3,
RLTD_PRTY_ID = EXPTRANS1.lkp_RLTD_PRTY_ID3,
PRTY_RLTD_ROLE_CD = EXPTRANS1.lkp_PRTY_RLTD_ROLE_CD3,
EDW_STRT_DTTM = EXPTRANS1.lkp_EDW_STRT_DTTM3,
EDW_END_DTTM = EXPTRANS1.Expiring_EndDate,
TRANS_END_DTTM = EXPTRANS1.Trans_strt_dttm31;


-- PIPELINE END FOR 1

-- PIPELINE START FOR 2

-- Component SQ_pc_prty_rltd_distadjuster, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_pc_prty_rltd_distadjuster AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as Relationship,
$2 as PNI_AddressBookUID,
$3 as OTHER_AddressBookUID,
$4 as Code,
$5 as Name,
$6 as EffectiveDate,
$7 as ExpirationDate,
$8 as Lookups,
$9 as SYS_SRC_CD,
$10 as updatetime,
$11 as Retired,
$12 as Party_Structure_Type_Cd,
$13 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
/*********************************** Claims Hierarchy Start***********************************/

/** Use INT ORG to get parent and child PARTY ID***/

 SELECT DISTINCT

	Relationship as Relationship, 

	PNI_AddressBookUID as PNI_AddressBookUID, 

	OTHER_AddressBookUID as OTHER_AddressBookUID, 

	Code as Code, 

	Name1 as Name1, 

	 cast(eff_dt as date) as EffectiveDate, 

	 end_dt as ExpirationDate, 

	 Lookups as Lookups,

	''SRC_SYS4'' AS SYS_SRC_CD,

	 updatetime,

	 Retired,Party_Structure_Type_Cd from (

SELECT DISTINCT 

cast(case when cct.TYPECODE_stg=''district_alfa'' and childtype.TYPECODE_stg=''servicecenter_alfa'' then ''PRTY_RLTD_ROLE24''

when cct.TYPECODE_stg=''district_alfa'' and childtype.TYPECODE_stg=''stormcenter_alfa'' then ''PRTY_RLTD_ROLE41''

when cct.TYPECODE_stg=''general'' and childtype.TYPECODE_stg=''general'' then ''PRTY_RLTD_ROLE42''

when cct.TYPECODE_stg=''general'' and childtype.TYPECODE_stg=''medical_mgmt'' then ''PRTY_RLTD_ROLE44''

when cct.TYPECODE_stg=''general'' and childtype.TYPECODE_stg=''region_alfa'' then ''PRTY_RLTD_ROLE43''

when cct.TYPECODE_stg=''region_alfa'' and childtype.TYPECODE_stg=''district_alfa'' then ''PRTY_RLTD_ROLE23''

when cct.TYPECODE_stg=''region_alfa'' and childtype.TYPECODE_stg=''general'' then ''PRTY_RLTD_ROLE45''

when cct.TYPECODE_stg=''root'' and childtype.TYPECODE_stg=''general'' then ''PRTY_RLTD_ROLE46''

when cct.TYPECODE_stg=''district_alfa'' and childtype.TYPECODE_stg=''general'' then ''PRTY_RLTD_ROLE47''

else  NULL end as varchar(50)) as Relationship,

/* ''PRTY_RLTD_ROLE4'' as Relationship, -- Need to add value in XLAT table */
cast(cct.TYPECODE_stg as varchar(100)) AS PNI_AddressBookUID,

cast(ccg.name_stg as varchar(100))AS OTHER_AddressBookUID,

cast(childtype.TYPECODE_stg as varchar(100))AS Code,

child.name_stg AS Name1,

CAST(''1990-01-01 00:00:00.000000'' AS timestamp(6)) AS eff_dt, /*  Need to follow up with Ahmed			 */
CAST(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS end_dt, /*  Need to follow up with Ahmed		 */
cast(current_timestamp as timestamp(6))  AS UpdateTime,

cast(''INTRNL_ORG'' as varchar(50))as Lookups,  

 case when ccg.retired_stg=0 and child.retired_stg=0 and cct.retired_stg=0 and childtype.retired_stg=0 then 0 else 1 end as Retired,

cast(''PRTY_STRC_TYPE15'' as varchar(50)) AS Party_Structure_Type_Cd

FROM

DB_T_PROD_STAG.cc_parentgroup ccp inner join DB_T_PROD_STAG.cc_group ccg on ccg.id_stg=ccp.ForeignEntityID_stg

inner join DB_T_PROD_STAG.cctl_grouptype cct on cct.id_stg=ccg.GroupType_stg

inner join DB_T_PROD_STAG.cc_group child on child.id_stg=ccp.OwnerID_stg

inner join DB_T_PROD_STAG.cctl_grouptype childtype on childtype.id_stg=child.GroupType_stg

where not (cct.TYPECODE_stg = ''region_alfa'' and childtype.TYPECODE_stg = ''servicecenter_alfa'')

 union

/**District or Storm Center associated with the adjuster relationship****/

SELECT DISTINCT

	''PRTY_RLTD_ROLE12'' AS Relationship, 

	cctl_grouptypecode,

	cc_group_name AS PNI_AddressBookUID,

	cc_contact_publicID AS OTHER_AddressBookUID,

	cast(NULL as varchar(30)) AS Name1,

	CAST(''1900-01-01 00:00:00.000000'' AS timestamp(6)) AS eff_dt, 

	CAST(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS END_dt, 

	CAST(''1900-01-01 00:00:00.000000'' AS timestamp(6)) AS UpdateTime,

	''INTRNL_ORG Claim_INDIV'' AS Lookups, 

	CASE WHEN usersgroup_retired=0 and cc_contact_retired=0  then 0 else 1 END AS Retired,

	''PRTY_STRC_TYPE9''  AS Party_Structure_Type_Cd

	FROM 

	(SELECT 

			cgu.UserID_stg AS adjusterID,

			cct.TYPECODE_stg AS cctl_grouptypecode,

			cg.Name_stg AS cc_group_name,

			cc.PublicID_stg AS cc_contact_publicID,

			cg.retired_stg AS usersgroup_retired,

			cc.retired_stg AS cc_contact_retired,

			rank() over (Partition by cu.AdjusterCode_Alfa_stg order by CASE WHEN left(cu.adjusterCode_alfa_stg,1)=  CASE WHEN cct.Name_stg = ''District'' then left(cg.name_stg,1)

 			END or right(cu.adjusterCode_alfa_stg,1)= CASE WHEN cct.Name_stg = ''District'' then left(cg.name_stg,1)

END  then 1 else 2 END) AS rankDistrict, /* EIM-49995 */
 adjustercode_alfa_stg

 from DB_T_PROD_STAG.cc_user cu

 INNER join DB_T_PROD_STAG.cc_contact cc ON cc.id_stg=cu.ContactID_stg

 LEFT join DB_T_PROD_STAG.cc_groupuser cgu on cgu.UserID_stg = cu.id_stg 

			LEFT join DB_T_PROD_STAG.cc_group cg ON cg.id_stg=cgu.GroupID_stg

			LEFT join DB_T_PROD_STAG.cctl_grouptype cct  ON cct.id_stg =cg.GroupType_stg

			WHERE cct.Name_stg in (''District'')

		)adjusterDistricts where  rankDistrict=1

 and adjustercode_alfa_stg is not null



union



/* Claim Hierarchy District/regional Manager */

SELECT

''PRTY_RLTD_ROLE25''  as Relationship, /*  Need to add value in XLAT table for PRTY_RLTD_ROLE */
cct.TYPECODE_stg,

cg.name_stg as PNI_AddressBookUID, /*  Can load value in this way as GRoup. */
cc.PublicID_stg AS OTHER_AddressBookUID,

cast(NULL as varchar(30)) AS Name1,

cg.createtime_stg as EffectiveDate,

cast(NULL as timestamp(6)) as ExpirationDate,

cc.UpdateTime_stg AS Updatetime,

''INTRNL_ORG Claim_INDIV'' as Lookups, 

 case when cu.retired_stg=0 and cg.retired_stg=0 and cc.retired_stg=0 and cct.retired_stg=0 then 0 else 1 end as Retired,

''PRTY_STRC_TYPE5'' AS Party_Structure_Type_Cd

from DB_T_PROD_STAG.cc_group cg 

 inner join DB_T_PROD_STAG.cc_groupuser cgu on cgu.groupid_stg = cg.id_stg

inner join DB_T_PROD_STAG.cctl_grouptype cct on cct.id_stg=cg.GroupType_stg

inner join DB_T_PROD_STAG.cc_user cu on cu.id_stg=cgu.UserID_stg and cgu.Manager_stg=1

inner join DB_T_PROD_STAG.cc_contact cc on cc.id_stg=cu.ContactID_stg

where cct.TYPECODE_stg in (''region_alfa'')



union



SELECT

''PRTY_RLTD_ROLE27''  as Relationship, /*  Need to add value in XLAT table for PRTY_RLTD_ROLE */
cct.TYPECODE_stg,

cg.name_stg as PNI_AddressBookUID, /*  Can load value in this way as GRoup. */
cc.PublicID_stg AS OTHER_AddressBookUID,

cast(NULL as varchar(30)) AS Name1,

cg.createtime_stg as EffectiveDate,

cast(NULL as timestamp(6)) as ExpirationDate,

cg.UpdateTime_stg AS Updatetime,

''INTRNL_ORG Claim_INDIV'' as Lookups, 

 case when cu.retired_stg=0 and cg.retired_stg=0 and cc.retired_stg=0 and cct.retired_stg=0 then 0 else 1 end as Retired,

''PRTY_STRC_TYPE5'' AS Party_Structure_Type_Cd

from DB_T_PROD_STAG.cc_group cg 

 inner join DB_T_PROD_STAG.cc_groupuser cgu on cgu.groupid_stg= cg.id_stg

inner join DB_T_PROD_STAG.cctl_grouptype cct on cct.id_stg=cg.GroupType_stg

inner join DB_T_PROD_STAG.cc_user cu on cu.id_stg=cgu.UserID_stg and cgu.Manager_stg=1

inner join DB_T_PROD_STAG.cc_contact cc on cc.id_stg=cu.ContactID_stg

where cct.TYPECODE_stg in (''district_alfa'')



union



/* Claim Hierarchy District/regional Supervisor */

SELECT

''PRTY_RLTD_ROLE28''  as Relationship, /*  Need to add value in XLAT table for PRTY_RLTD_ROLE */
  cct.TYPECODE_stg,

cg.name_stg as PNI_AddressBookUID, /*  Can load value in this way as GRoup. */
  cc.PublicID_stg AS OTHER_AddressBookUID,

  cast(NULL as varchar(30)) AS Name1,

  cg.createtime_stg as effectivedate,

  cast(NULL as timestamp(6)) as ExpirationDate,

  cg.UpdateTime_stg AS Updatetime,

  ''INTRNL_ORG Claim_INDIV'' as Lookups, 

  case when cu.retired_stg=0 and cg.retired_stg=0 and cc.retired_stg=0 and cct.retired_stg=0 then 0 else 1 end as Retired,

''PRTY_STRC_TYPE5'' AS Party_Structure_Type_Cd

from DB_T_PROD_STAG.cc_group cg 

 inner join DB_T_PROD_STAG.cctl_grouptype cct on cct.id_stg=cg.GroupType_stg

inner join DB_T_PROD_STAG.cc_user cu on cu.id_stg=cg.SupervisorID_stg

inner join DB_T_PROD_STAG.cc_contact cc on cc.id_stg=cu.ContactID_stg

where cct.TYPECODE_stg in (''district_alfa'')

/* order by TYPECODE,cc_group.Name,cc_group.UpdateTime */


union



SELECT

''PRTY_RLTD_ROLE25''  as Relationship, /*  Need to add value in XLAT table for PRTY_RLTD_ROLE */
  cct.TYPECODE_stg,

cg.name_stg as PNI_AddressBookUID, /*  Can load value in this way as GRoup. */
  cc.PublicID_stg AS OTHER_AddressBookUID,

  cast(NULL as varchar(30)) AS Name1,

  cg.createtime_stg as effectivedate,

  cast(NULL as timestamp(6)) as ExpirationDate,

  cg.UpdateTime_stg AS Updatetime,

  ''INTRNL_ORG Claim_INDIV'' as Lookups, 

  case when cu.retired_stg=0 and cg.retired_stg=0 and cc.retired_stg=0 and cct.retired_stg=0 then 0 else 1 end as Retired,

''PRTY_STRC_TYPE5'' AS Party_Structure_Type_Cd

from DB_T_PROD_STAG.cc_group cg 

 inner join DB_T_PROD_STAG.cctl_grouptype cct on cct.id_stg=cg.GroupType_stg

inner join DB_T_PROD_STAG.cc_user cu on cu.id_stg=cg.SupervisorID_stg

inner join DB_T_PROD_STAG.cc_contact cc on cc.id_stg=cu.ContactID_stg

where cct.TYPECODE_stg in (''region_alfa'')





/*********************************** Claims Hierarchy END***********************************/



union



/* Agent to Producer Area */	



-- in doc its 			pc_userproducercode  date not present****/												 */
SELECT DISTINCT																

cast(''PRTY_RLTD_ROLE10'' as varchar(50)) as Relationship, 															

pc.PublicID_stg,/* -- Party ID from Individual										 */
pp.Code_stg,/* --- Related Party ID from Internal Org						 */
cast(NULL as varchar(30)) AS Code,																

cast(''INTRNL_ORG_SBTYPE2'' as varchar(30)) AS Name1,																

CAST(''1990-01-01 00:00:00.000000'' AS timestamp(6)) AS eff_dt, /*  Need to follow up with Ahmed			 */
CAST(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS end_dt, /*  Need to follow up with Ahmed		 */
cast(current_timestamp as timestamp(6))  AS UpdateTime,																	

cast(''Claim_INDIV INTRNL_ORG'' as varchar(50)) as Lookups, 													

case when pp.retired_stg=0 and pc.retired_stg=0 and pcc.retired_stg=0 and pr.retired_stg=0 then 0 else 1 end as Retired,	

cast(''PRTY_STRC_TYPE16'' as varchar(50)) AS Structure 

from DB_T_PROD_STAG.pc_producercode	pp															

left outer join DB_T_PROD_STAG.pc_userproducercode pup on  pp.id_stg = pup.producercodeid_stg														

left outer join DB_T_PROD_STAG.pc_user pu on  pup.UserID_stg = pu.id_stg						

left outer join DB_T_PROD_STAG.pc_userrole pur on pu.id_stg = pur.UserID_stg									

left outer join DB_T_PROD_STAG.pc_role UserRole on pur.RoleID_stg = UserRole.ID_stg						

left outer join DB_T_PROD_STAG.pc_contact pc on  pu.ContactID_stg = pc.id_stg								

left outer join DB_T_PROD_STAG.pctl_contact pcc on  pc.Subtype_stg = pcc.ID_stg							

left outer join DB_T_PROD_STAG.pc_producercoderole ppr on  pp.id_stg = ppr.ProducerCodeID_stg															

left outer join DB_T_PROD_STAG.pc_role pr on  ppr.RoleID_stg = pr.ID_stg							

left outer join DB_T_PROD_STAG.pc_role ProducerCodeRole on  ppr.RoleID_stg = ProducerCodeRole.ID_stg																

where pcc.name_stg=''User Contact''															

AND pr.name_stg=''Agent''																

-- AND UserRole.name_stg = ''Agent''	 /* EIM - 36268 */														 */
UNION																

/****Party related (Primary Named Insured and Prior Carrier******/								

SELECT DISTINCT																

''PRTY_RLTD_ROLE3'' as Relationship, /*  Need to get confirmation from Keitra			 */
pc.AddressBookUID_stg, /* Party ID from Policy_INDIVidual							 */
ppa.TYPECODE_stg, /* Related party ID from Business,						 */
	cast(NULL as varchar(30)) AS Code, 													

	cast(NULL as varchar(30)) AS Name1,															

CAST(''1990-01-01 00:00:00.000000'' AS timestamp(6)) AS eff_dt, /*  Need to follow up with Ahmed			 */
CAST(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS end_dt,/*  Need to follow up with Ahmed		 */
pp.updatetime_stg AS UpdateTime,															

	''BUSN Policy_INDIV'' as Lookups, 															

case when pc.retired_stg=0 and ppa.retired_stg=0 and pcc.retired_stg=0 and pp.retired_stg=0 and pj.retired_stg=0 then 0 else 1 end as Retired,																

cast(NULL as varchar(50)) AS Structure																

from DB_T_PROD_STAG.pc_policycontactrole ppc 															

	INNER JOIN DB_T_PROD_STAG.pc_contact pc ON pc.id_stg=ppc.ContactDenorm_stg															

	INNER JOIN DB_T_PROD_STAG.pctl_policycontactrole ppcp ON ppcp.id_stg=ppc.Subtype_stg															

	INNER JOIN DB_T_PROD_STAG.pctl_contact pcc ON pcc.id_stg=pc.Subtype_stg	 														

	INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp ON pp.id_stg=ppc.BranchID_stg															

	INNER JOIN DB_T_PROD_STAG.pc_effectivedatedfields pe ON pp.id_stg=pe.BranchID_stg															

	INNER JOIN DB_T_PROD_STAG.pc_job pj ON pj.id_stg=pp.JobID_stg															

	INNER JOIN DB_T_PROD_STAG.pctl_job pcj ON pcj.id_stg=pj.Subtype_stg															

	INNER JOIN DB_T_PROD_STAG.pctl_policyperiodstatus pps ON pps.id_stg=pp.Status_stg															

	INNER JOIN DB_T_PROD_STAG.pctl_priorcarrier_alfa ppa ON ppa.id_stg=pe.PriorCarrier_alfa_stg															

WHERE 																

	pps.TYPECODE_stg=''Bound'' AND 															

																

	ppcp.TYPECODE_stg=''PolicyPriNamedInsured''AND															

pe.ExpirationDate_stg IS NULL /* AND															 */
/* pc_effectivedatedfields.UpdateTime > 						 */


  AND (  ( ppc.UpdateTime_stg > (:start_dttm) and ppc.UpdateTime_stg <= (:end_dttm))

 or ( pc.UpdateTime_stg > (:start_dttm) and pc.UpdateTime_stg <= (:end_dttm))

 or ( pp.UpdateTime_stg > (:start_dttm) and pp.UpdateTime_stg <= (:end_dttm))

 or ( pj.UpdateTime_stg > (:start_dttm) and pj.UpdateTime_stg<= (:end_dttm))

 or ( pe.UpdateTime_stg > (:start_dttm) and pe.UpdateTime_stg <= (:end_dttm))  )									

union																											

/***********************************Sales Hierarchy Start***********************************/

/** Sales Hierarchy ALFA to State **/



-- in doc its 			pc_parentgroup/*  date not present****/	 */


/* Use INT ORG to get parent and child PARTY ID */
SELECT DISTINCT

''PRTY_RLTD_ROLE29'' as Relationship, /*  Need to add value in XLAT table */
pcg.TYPECODE_stg AS Subtype,

pg.name_stg as code,

''INTRNL_ORG_SBTYPE4'' AS Name1,

prz.code_stg AS Subtype,

CAST(''1990-01-01 00:00:00.000000'' AS timestamp(6)) AS eff_dt, /*  Need to follow up with Ahmed			 */
CAST(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS end_dt, /*  Need to follow up with Ahmed		 */
cast(current_timestamp as timestamp(6))  AS UpdateTime,		

''INTRNL_ORG'' as Lookups, 

 case when pg.retired_stg=0 and child.retired_stg=0 and district.retired_stg=0 then 0 else 1 end as Retired,

''PRTY_STRC_TYPE16'' AS Structure  /* SLSHRCHY */
FROM

/**************ALFA to MARKETING************/

DB_T_PROD_STAG.pc_parentgroup ppg INNER JOIN DB_T_PROD_STAG.pc_group pg on pg.id_stg=ppg.ForeignEntityID_stg

INNER JOIN DB_T_PROD_STAG.pc_group child on child.id_stg=ppg.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pctl_grouptype pcg on pcg.id_stg=pg.GroupType_stg

INNER JOIN DB_T_PROD_STAG.pctl_grouptype childgrouptype on childgrouptype.id_stg=child.GroupType_stg

/************Marketing to District************/

INNER JOIN DB_T_PROD_STAG.pc_parentgroup pg_marketing_district on pg_marketing_district.ForeignEntityID_stg=ppg.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pc_group district on district.id_stg=pg_marketing_district.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pctl_grouptype district_type on district_type.id_stg=district.GroupType_stg

/********District to State*****************/

INNER JOIN DB_T_PROD_STAG.pc_groupregion pgr on pgr.GroupID_stg=pg_marketing_district.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pc_region_zone prz on pgr.RegionID_stg=prz.RegionId_stg

where 

(pcg.TYPECODE_stg=''root'' and childgrouptype.TYPECODE_stg=''region'') 



UNION



/** Sales Hierarchy State to Marketing **/

/* Use INT ORG to get parent and child PARTY ID */
SELECT DISTINCT

''PRTY_RLTD_ROLE22'' as Relationship, /*  Need to add value in XLAT table */
  ''INTRNL_ORG_SBTYPE4'' AS Name1,

  prz.code_stg,

childgrouptype.TYPECODE_stg,

child.name_stg AS OTHER_AddressBookUID,

CAST(''1990-01-01 00:00:00.000000'' AS timestamp(6)) AS eff_dt, /*  Need to follow up with Ahmed			 */
CAST(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS end_dt, /*  Need to follow up with Ahmed		 */
cast(current_timestamp as timestamp(6))  AS UpdateTime,		

''INTRNL_ORG'' as Lookups, 

 case when pg.retired_stg=0 and child.retired_stg=0 and district.retired_stg=0 then 0 else 1 end as Retired,

''PRTY_STRC_TYPE16'' AS Structure  /* SLSHRCHY */
FROM

/**************ALFA to MARKETING************/

DB_T_PROD_STAG.pc_parentgroup ppg INNER JOIN DB_T_PROD_STAG.pc_group pg on pg.id_stg=ppg.ForeignEntityID_stg

INNER JOIN DB_T_PROD_STAG.pc_group child on child.id_stg=ppg.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pctl_grouptype pcg on pcg.id_stg=pg.GroupType_stg

INNER JOIN DB_T_PROD_STAG.pctl_grouptype childgrouptype on childgrouptype.id_stg=child.GroupType_stg

/************Marketing to District************/

INNER JOIN DB_T_PROD_STAG.pc_parentgroup pg_marketing_district on pg_marketing_district.ForeignEntityID_stg=ppg.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pc_group district on district.id_stg=pg_marketing_district.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pctl_grouptype district_type on district_type.id_stg=district.GroupType_stg

/********District to State*****************/

INNER JOIN DB_T_PROD_STAG.pc_groupregion pgr on pgr.GroupID_stg=pg_marketing_district.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pc_region_zone prz on pgr.RegionID_stg=prz.RegionId_stg

where 

(pcg.TYPECODE_stg=''root'' and childgrouptype.TYPECODE_stg=''region'') 

 union

/* All Supervisors*/

SELECT DISTINCT

case when childgrouptype.TYPECODE_stg=''custserv'' then ''PRTY_RLTD_ROLE53'' 

when childgrouptype.TYPECODE_stg=''homeofficeadmin'' then ''PRTY_RLTD_ROLE54''

when childgrouptype.TYPECODE_stg=''region'' then ''PRTY_RLTD_ROLE26''

when childgrouptype.TYPECODE_stg=''salesdistrict_alfa'' then ''PRTY_RLTD_ROLE28'' 

when childgrouptype.TYPECODE_stg=''servicecenter_alfa'' then ''PRTY_RLTD_ROLE31''

when childgrouptype.TYPECODE_stg=''underwritingdistrict_alfa'' then ''PRTY_RLTD_ROLE28''

when childgrouptype.TYPECODE_stg=''homeofficeuw'' then ''PRTY_RLTD_ROLE55''

else NULL end as Relationship,

childgrouptype.TYPECODE_stg,

child.name_stg AS PNI_AddressBookUID,

  pc.PublicID_stg AS OTHER_AddressBookUID,

  cast(NULL as varchar(30)) AS Name1,

CAST(''1990-01-01 00:00:00.000000'' AS timestamp(6)) AS eff_dt, /*  Need to follow up with Ahmed			 */
CAST(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS end_dt, /*  Need to follow up with Ahmed		 */
cast(current_timestamp as timestamp(6))  AS UpdateTime,		

''INTRNL_ORG Claim_INDIV'' as Lookups, 

 case when pg.retired_stg=0 and child.retired_stg=0 and pu.retired_stg=0 and pc.retired_stg=0 then 0 else 1 end as Retired,

case when childgrouptype.TYPECODE_stg in(''underwritingdistrict_alfa'',''homeofficeuw'') then ''PRTY_STRC_TYPE14''

else ''PRTY_STRC_TYPE17'' end as Structure  /* SLSPRSASSGN */
from DB_T_PROD_STAG.pc_parentgroup ppg INNER JOIN DB_T_PROD_STAG.pc_group pg on pg.id_stg=ppg.ForeignEntityID_stg

INNER JOIN DB_T_PROD_STAG.pc_group child on child.id_stg=ppg.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pc_user pu on pu.id_stg=child.SupervisorID_stg

INNER JOIN DB_T_PROD_STAG.pc_contact pc on pc.id_stg=pu.ContactID_stg

INNER JOIN DB_T_PROD_STAG.pctl_grouptype pcg on pcg.id_stg=pg.GroupType_stg

INNER JOIN DB_T_PROD_STAG.pctl_grouptype childgrouptype on childgrouptype.id_stg=child.GroupType_stg

/* where  */
/* (pctl_grouptype.TYPECODE=''root'' and childgrouptype.TYPECODE=''region'')  */
union

/* All Managers*/

SELECT DISTINCT

case when childgrouptype.TYPECODE_stg=''custserv'' then ''PRTY_RLTD_ROLE51'' 

when childgrouptype.TYPECODE_stg=''homeofficeadmin'' then ''PRTY_RLTD_ROLE52''

when childgrouptype.TYPECODE_stg=''region'' then ''PRTY_RLTD_ROLE25''

when childgrouptype.TYPECODE_stg=''salesdistrict_alfa'' then ''PRTY_RLTD_ROLE27''

when childgrouptype.TYPECODE_stg=''servicecenter_alfa'' then ''PRTY_RLTD_ROLE32''

when childgrouptype.TYPECODE_stg=''underwritingdistrict_alfa'' then ''PRTY_RLTD_ROLE27''

when childgrouptype.TYPECODE_stg=''homeofficeuw'' then ''PRTY_RLTD_ROLE56''

else NULL end as Relationship,

/* ''RGNTOMGR'' as Relationship1, -- Need to add value in XLAT table */
childgrouptype.TYPECODE_stg,

child.name_stg AS PNI_AddressBookUID,

  pc.PublicID_stg AS OTHER_AddressBookUID,

  cast(NULL as varchar(30)) AS Name1,

CAST(''1990-01-01 00:00:00.000000'' AS timestamp(6)) AS eff_dt, /*  Need to follow up with Ahmed			 */
CAST(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS end_dt, /*  Need to follow up with Ahmed		 */
cast(current_timestamp as timestamp(6))  AS UpdateTime,	

''INTRNL_ORG Claim_INDIV'' as Lookups, 

 case when pg.retired_stg=0 and child.retired_stg=0 and pu.retired_stg=0 and pc.retired_stg=0 then 0 else 1 end as Retired,

case when childgrouptype.TYPECODE_stg in(''underwritingdistrict_alfa'',''homeofficeuw'') then ''PRTY_STRC_TYPE14''

else ''PRTY_STRC_TYPE17'' end as Structure  /* SLSPRSASSGN */
from DB_T_PROD_STAG.pc_parentgroup ppg INNER JOIN DB_T_PROD_STAG.pc_group pg on pg.id_stg=ppg.ForeignEntityID_stg

INNER JOIN DB_T_PROD_STAG.pc_group child on child.id_stg=ppg.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pc_groupuser pgu on pgu.GroupID_stg=child.id_stg and pgu.Manager_stg=1

INNER JOIN DB_T_PROD_STAG.pc_user pu on pu.id_stg=pgu.UserID_stg 

 INNER JOIN DB_T_PROD_STAG.pc_contact pc on pc.id_stg=pu.ContactID_stg

INNER JOIN DB_T_PROD_STAG.pctl_grouptype pcg on pcg.id_stg=pg.GroupType_stg

INNER JOIN DB_T_PROD_STAG.pctl_grouptype childgrouptype on childgrouptype.id_stg=child.GroupType_stg



union



/* Region to District*/

SELECT DISTINCT

''PRTY_RLTD_ROLE23'' as Relationship, /*  Need to add value in XLAT table  */
 pcg.TYPECODE_stg,

pg.name_stg as PNI_AddressBookUID,

childgrouptype.TYPECODE_stg,

child.name_stg AS OTHER_AddressBookUID,

CAST(''1990-01-01 00:00:00.000000'' AS timestamp(6)) AS eff_dt, /*  Need to follow up with Ahmed			 */
CAST(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS end_dt, /*  Need to follow up with Ahmed		 */
cast(current_timestamp as timestamp(6))  AS UpdateTime,	

''INTRNL_ORG'' as Lookups, 

 case when pg.retired_stg=0 and child.retired_stg=0 then 0 else 1 end as Retired,

''PRTY_STRC_TYPE16'' AS Structure  /*  SLSHRCY */
from DB_T_PROD_STAG.pc_parentgroup ppg INNER JOIN DB_T_PROD_STAG.pc_group pg on pg.id_stg=ppg.ForeignEntityID_stg

INNER JOIN DB_T_PROD_STAG.pc_group child on child.id_stg=ppg.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pctl_grouptype pcg on pcg.id_stg=pg.GroupType_stg

  INNER JOIN DB_T_PROD_STAG.pctl_grouptype childgrouptype on childgrouptype.id_stg=child.GroupType_stg

where 

(pcg.TYPECODE_stg=''region'' and childgrouptype.TYPECODE_stg=''salesdistrict_alfa'') 

/* OR (pctl_grouptype.TYPECODE=''salesdistrict_alfa'' and childgrouptype.TYPECODE=''servicecenter_alfa'') */
union



/*District to SVC*/



SELECT DISTINCT

''PRTY_RLTD_ROLE24'' as Relationship, /*  Need to add value in XLAT table  */
 pcg.TYPECODE_stg,

pg.name_stg as PNI_AddressBookUID,

childgrouptype.TYPECODE_stg,

child.name_stg AS OTHER_AddressBookUID,

CAST(''1990-01-01 00:00:00.000000'' AS timestamp(6)) AS eff_dt, /*  Need to follow up with Ahmed			 */
CAST(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS end_dt, /*  Need to follow up with Ahmed		 */
cast(current_timestamp as timestamp(6))  AS UpdateTime,

''INTRNL_ORG'' as Lookups, 

 case when pg.retired_stg=0 and child.retired_stg=0 then 0 else 1 end as Retired,

''PRTY_STRC_TYPE16'' AS Structure  /*  SLSHRCY */
from DB_T_PROD_STAG.pc_parentgroup ppg INNER JOIN DB_T_PROD_STAG.pc_group pg on pg.id_stg=ppg.ForeignEntityID_stg

INNER JOIN DB_T_PROD_STAG.pc_group child on child.id_stg=ppg.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pctl_grouptype pcg on pcg.id_stg=pg.GroupType_stg

  INNER JOIN DB_T_PROD_STAG.pctl_grouptype childgrouptype on childgrouptype.id_stg=child.GroupType_stg

where 

/* (pctl_grouptype.TYPECODE=''region'' and childgrouptype.TYPECODE=''salesdistrict_alfa'') or */
(pcg.TYPECODE_stg=''salesdistrict_alfa'' and childgrouptype.TYPECODE_stg=''servicecenter_alfa'')

UNION



/*SELECT DISTINCT

pc_role.Name as Relationship, --  Need to add value in XLAT table 
''INTRNL_ORG_SBTYPE2'' AS Subtype,

pc_producercode.code AS PNI_AddressBookUID,

  pc_contact.PublicID AS OTHER_AddressBookUID,

  NULL AS Name,

convert(datetime, ''01/01/1990'', 101) AS eff_dt, --  Need to follow up with Ahmed			 
CAST(''9999-12-31 23:59:59.9999999'' AS datetime2(7)) AS end_dt, --  Need to follow up with Ahmed		 
 getdate() AS UpdateTime,	

''INTRNL_ORG Policy_INDIV'' as Lookups, 

 case when pc_producercode.retired=0 and pc_user.retired=0 and pc_contact.retired=0 then 0 else 1 end as Retired,

''PRTY_STRC_TYPE17'' AS Structure  -- --SLSPRSASSGN 
,(:start_dttm) as start_dttm,

(:end_dttm) as end_dttm

from DB_T_PROD_STAG.pc_producercode INNER JOIN DB_T_PROD_STAG.pc_userproducercode on pc_userproducercode.ProducerCodeID=pc_producercode.id

inner join DB_T_PROD_STAG.pc_user on pc_user.id=pc_userproducercode.UserID 

 INNER JOIN DB_T_PROD_STAG.pc_contact on pc_contact.id=pc_user.ContactID

INNER JOIN DB_T_PROD_STAG.pc_userrole on pc_userrole.UserID=pc_user.ID

INNER JOIN DB_T_PROD_STAG.pc_role on pc_role.id=pc_userrole.RoleID

where pc_role.Name in (''Agent'',''CSR'')*/



/****Adding as fix for EIM-13666- Ankit- 5/30/2017*********************/

/** SVC to Role **/

SELECT DISTINCT

i.Name_stg as Relationship, /*  Need to add value in XLAT table */
b.typecode_stg AS Subtype,

a.NameDenorm_stg AS PNI_AddressBookUID,

/* a.NameDenorm_stg collate  SQL_Latin1_General_CP1_CI_AS AS PNI_AddressBookUID, */
  e.PublicID_stg AS OTHER_AddressBookUID,

  cast(NULL as varchar(30)) AS Name1,

CAST(''1990-01-01 00:00:00.000000'' AS timestamp(6)) AS eff_dt, /*  Need to follow up with Ahmed			 */
CAST(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS end_dt, /*  Need to follow up with Ahmed		 */
cast(current_timestamp as timestamp(6))  AS UpdateTime,

''INTRNL_ORG Policy_INDIV'' as Lookups, 

 case when a.retired_stg=0 and d.retired_stg=0 and e.retired_stg=0 then 0 else 1 end as Retired,

''PRTY_STRC_TYPE17'' AS Structure  /* --SLSPRSASSGN */
from DB_T_PROD_STAG.pc_group a

INNER JOIN DB_T_PROD_STAG.pctl_grouptype b on b.id_stg = a.GroupType_stg

INNER JOIN DB_T_PROD_STAG.pc_groupuser c on c.GroupID_stg = a.id_stg

INNER JOIN DB_T_PROD_STAG.pc_user d on d.id_stg = c.UserID_stg

INNER JOIN DB_T_PROD_STAG.pc_contact e on e.id_stg = d.ContactID_stg 

INNER JOIN DB_T_PROD_STAG.pc_userrole h on h.UserID_stg = d.id_stg

INNER JOIN DB_T_PROD_STAG.pc_role i on i.id_stg = h.RoleID_stg

where b.typecode_stg = ''servicecenter_alfa''

/* and i.name_stg <> ''CRC Team Leaders'' */


UNION



/** Sales Hierarchy Producer to User **/

SELECT DISTINCT

''PRTY_RLTD_ROLE91'' as Relationship, /*  Need to add value in XLAT table */
''INTRNL_ORG_SBTYPE2'' AS Subtype,

 pp.code_stg AS PNI_AddressBookUID,

  pc.PublicID_stg AS OTHER_AddressBookUID,

  cast(NULL as varchar(30)) AS Name1,

CAST(''1990-01-01 00:00:00.000000'' AS timestamp(6)) AS eff_dt, /*  Need to follow up with Ahmed			 */
CAST(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS end_dt, /*  Need to follow up with Ahmed		 */
cast(current_timestamp as timestamp(6))  AS UpdateTime,	

''INTRNL_ORG Policy_INDIV'' as Lookups, 

 case when pp.retired_stg=0 and pu.retired_stg=0 and pc.retired_stg=0 then 0 else 1 end as Retired,

''PRTY_STRC_TYPE17'' AS Structure /* --SLSPRSASSGN */
from DB_T_PROD_STAG.pc_producercode pp INNER JOIN DB_T_PROD_STAG.pc_userproducercode pup on pup.ProducerCodeID_stg=pp.id_stg

inner join DB_T_PROD_STAG.pc_user pu on pu.id_stg=pup.UserID_stg

 INNER JOIN DB_T_PROD_STAG.pc_contact pc on pc.id_stg=pu.ContactID_stg



UNION



/* Producer Area to Service center */

SELECT Relationship,TYPECODE1,PNI_AddressBookUID,TYPECODE2,OTHER_AddressBookUID,eff_dt,end_dt,UpdateTime,Lookups,Retired,Structure from 

(

SELECT DISTINCT

''PRTY_RLTD_ROLE30'' AS Relationship, 

 ''INTRNL_ORG_SBTYPE2'' AS TYPECODE1,

pp.Code_stg AS PNI_AddressBookUID, /* --- Party ID from Internal Org */
pcg.TYPECODE_stg AS TYPECODE2,

pg.NameDenorm_stg AS OTHER_AddressBookUID,  /* --- Related Party ID from Internal Org    */
/* pc_group.NameDenorm_stg collate  SQL_Latin1_General_CP1_CI_AS AS OTHER_AddressBookUID, */
CAST(''1990-01-01 00:00:00.000000'' AS timestamp(6)) AS eff_dt, /*  Need to follow up with Ahmed           */
CAST(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS end_dt, /*  Need to follow up with Ahmed   */
 pg.UpdateTime_stg AS UpdateTime, 

 ''INTRNL_ORG'' as Lookups, 

 case when pp.retired_stg=0 then 0 else 1 end as Retired,  

''PRTY_STRC_TYPE16'' AS Structure,  /* --- SLSHRCY */
        ROW_NUMBER() OVER (PARTITION BY pp.Code_stg  ORDER BY pg.UpdateTime_stg DESC) AS RN 

from DB_T_PROD_STAG.pc_group pg 

 join DB_T_PROD_STAG.pc_groupuser pgu on pg.ID_stg=pgu.GroupID_stg

join DB_T_PROD_STAG.pc_user pu on pgu.UserID_stg=pu.ID_stg

join DB_T_PROD_STAG.pc_contact pc on pu.ContactID_stg=pc.ID_stg

join DB_T_PROD_STAG.pctl_grouptype pcg on pg.GroupType_stg=pcg.ID_stg

/*  join DB_T_PROD_STAG.pctl_usertype pcu on pcu.ID_stg=pu.UserType_stg --EIM -35544 */
LEFT join DB_T_PROD_STAG.pc_userproducercode pup on pu.ID_stg=pup.UserID_stg

LEFT join DB_T_PROD_STAG.pc_producercode pp on pup.ProducerCodeID_stg=pp.ID_stg

left join DB_T_PROD_STAG.pc_producercoderole ppr on pp.id_stg = ppr.ProducerCodeID_stg  /*EIM -35544*/

left join DB_T_PROD_STAG.pc_role pr on pr.id_stg = ppr.roleid_stg  /*EIM -35544*/

where 

pcg.typecode_stg = ''servicecenter_alfa''

and pp.code_stg is not null

and pg.namedenorm_stg is not null

and pr.name_stg = ''Agent'' /*EIM - 35544*/

) Temp

WHERE Temp.RN = 1



/***********************************Sales Hierarchy END***********************************/



UNION



/*********************************** Underwriter Hierarchy Start***********************************/

/************UW Relationship between ALFA and UW Home Office*****/

SELECT DISTINCT

''PRTY_RLTD_ROLE33'' as Relationship, /*  Need to add value in XLAT table */
pcg.TYPECODE_stg,

pg.name_stg as PNI_AddressBookUID,

childgrouptype.typecode_stg,

child.name_stg AS OTHER_AddressBookUID,

CAST(''1990-01-01 00:00:00.000000'' AS timestamp(6)) AS eff_dt, /*  Need to follow up with Ahmed			 */
CAST(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS end_dt, /*  Need to follow up with Ahmed		 */
cast(current_timestamp as timestamp(6))  AS UpdateTime,

''INTRNL_ORG'' as Lookups, 

 case when pg.retired_stg=0 and child.retired_stg=0 then 0 else 1 end as Retired,

''PRTY_STRC_TYPE13'' AS Structure



from DB_T_PROD_STAG.pc_parentgroup ppg INNER JOIN DB_T_PROD_STAG.pc_group pg on pg.id_stg=ppg.ForeignEntityID_stg

INNER JOIN DB_T_PROD_STAG.pc_group child on child.id_stg=ppg.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pctl_grouptype pcg on pcg.id_stg=pg.GroupType_stg

  INNER JOIN DB_T_PROD_STAG.pctl_grouptype childgrouptype on childgrouptype.id_stg=child.GroupType_stg

where pcg.TYPECODE_stg in (''root'')

and childgrouptype.TYPECODE_stg in (''homeofficeuw'')



UNION



/**UW Office to State***/

SELECT DISTINCT

''PRTY_RLTD_ROLE34'' as Relationship, /*  Need to add value in XLAT table */
childgrouptype.typecode_stg,

child.name_stg AS OTHER_AddressBookUID,

''INTRNL_ORG_SBTYPE5'' AS Name,

prz.Code_stg,

CAST(''1990-01-01 00:00:00.000000'' AS timestamp(6)) AS eff_dt, /*  Need to follow up with Ahmed			 */
CAST(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS end_dt, /*  Need to follow up with Ahmed		 */
cast(current_timestamp as timestamp(6))  AS UpdateTime,

''INTRNL_ORG'' as Lookups, 

 case when pg.retired_stg=0 and child.retired_stg=0 and district.retired_stg=0 and pr.retired_stg=0 then 0 else 1 end as Retired,

''PRTY_STRC_TYPE13'' AS Structure

FROM

/** ALFA to UW Home Office **/

DB_T_PROD_STAG.pc_parentgroup ppg INNER JOIN DB_T_PROD_STAG.pc_group pg on pg.id_stg=ppg.ForeignEntityID_stg

INNER JOIN DB_T_PROD_STAG.pc_group child on child.id_stg=ppg.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pctl_grouptype pcg on pcg.id_stg=pg.GroupType_stg

  INNER JOIN DB_T_PROD_STAG.pctl_grouptype childgrouptype on childgrouptype.id_stg=child.GroupType_stg

/** UW Office to UW District **/  

INNER JOIN DB_T_PROD_STAG.pc_parentgroup ho_to_district on ho_to_district.ForeignEntityID_stg=ppg.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pc_group district on district.id_stg=ho_to_district.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pctl_grouptype districttype on districttype.id_stg=district.GroupType_stg

/** Distrit to Region***/

INNER JOIN DB_T_PROD_STAG.pc_groupregion pgr on pgr.GroupID_stg=ho_to_district.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pc_region pr on pr.id_stg=pgr.RegionID_stg

/**Region to State**/

INNER JOIN DB_T_PROD_STAG.pc_region_zone prz on prz.RegionId_stg=pgr.RegionID_stg

where pcg.TYPECODE_stg in (''root'')

and childgrouptype.TYPECODE_stg in (''homeofficeuw'')



UNION

/**UW State to District****/

SELECT DISTINCT

''PRTY_RLTD_ROLE35'' as Relationship, /*  Need to add value in XLAT table */
''INTRNL_ORG_SBTYPE5'' AS Name,

prz.Code_stg,

districttype.TYPECODE_stg,

district.name_stg,

CAST(''1990-01-01 00:00:00.000000'' AS timestamp(6)) AS eff_dt, /*  Need to follow up with Ahmed			 */
CAST(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS end_dt, /*  Need to follow up with Ahmed		 */
cast(current_timestamp as timestamp(6))  AS UpdateTime,	

''INTRNL_ORG'' as Lookups, 

 case when pg.retired_stg=0 and child.retired_stg=0 and district.retired_stg=0 and pr.retired_stg=0 then 0 else 1 end as Retired,

''PRTY_STRC_TYPE13'' AS Structure

FROM

/** ALFA to UW Home Office **/

DB_T_PROD_STAG.pc_parentgroup ppg INNER JOIN DB_T_PROD_STAG.pc_group pg on pg.id_stg=ppg.ForeignEntityID_stg

INNER JOIN DB_T_PROD_STAG.pc_group child on child.id_stg=ppg.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pctl_grouptype pcg on pcg.id_stg=pg.GroupType_stg

  INNER JOIN DB_T_PROD_STAG.pctl_grouptype childgrouptype on childgrouptype.id_stg=child.GroupType_stg

/** UW Office to UW District **/  

INNER JOIN DB_T_PROD_STAG.pc_parentgroup ho_to_district on ho_to_district.ForeignEntityID_stg=ppg.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pc_group district on district.id_stg=ho_to_district.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pctl_grouptype districttype on districttype.id_stg=district.GroupType_stg

/** Distrit to Region***/

INNER JOIN DB_T_PROD_STAG.pc_groupregion pgr on pgr.GroupID_stg=ho_to_district.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pc_region pr on pr.id_stg=pgr.RegionID_stg

/**Region to State**/

INNER JOIN DB_T_PROD_STAG.pc_region_zone prz on prz.RegionId_stg=pgr.RegionID_stg

where pcg.TYPECODE_stg in (''root'',''homeofficeuw'')

and childgrouptype.TYPECODE_stg in (''homeofficeuw'')

) as a WHERE

 	relationship IN (''PRTY_RLTD_ROLE12'',''PRTY_RLTD_ROLE41'',''PRTY_RLTD_ROLE24'',''PRTY_RLTD_ROLE43'',''PRTY_RLTD_ROLE23'',''PRTY_RLTD_ROLE45'',''PRTY_RLTD_ROLE22'',

 	''PRTY_RLTD_ROLE34'',''PRTY_RLTD_ROLE35'')



QUALIFY ROW_NUMBER() OVER (PARTITION BY PNI_AddressBookUID,OTHER_AddressBookUID,Relationship,Code,Name1,Lookups ORDER BY updatetime DESC)=1

ORDER BY effectivedate ASC,updatetime ASC

/*********************************** Underwriter Hierarchy END***********************************/
) SRC
)
);


-- Component exp_all_source11, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_all_source11 AS
(
SELECT
SQ_pc_prty_rltd_distadjuster.Relationship as TYPECODE,
DECODE ( TRUE , LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT */ IS NULL , SQ_pc_prty_rltd_distadjuster.Relationship , LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT */ ) as var_TYPECODE,
DECODE ( TRUE , var_TYPECODE IS NULL , ''UNK'' , var_TYPECODE ) as var1_TYPECODE,
LKP_3.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_INTRNL_ORG_SBTYPE */ as var_Code,
LKP_4.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_INTRNL_ORG_TYPE */ as INTRNL_ORG_TYPE_CD,
LKP_5.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_INTRNL_ORG_SBTYPE */ as INTRNL_ORG_SBTYPE_CD,
LKP_6.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_INTRNL_ORG_SBTYPE */ as var_Name,
LKP_7.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SRC_CD */ as v_SYS_SRC_CD,
DECODE ( TRUE , UPPER ( SQ_pc_prty_rltd_distadjuster.Lookups ) = ''BUSN POLICY_INDIV'' , DECODE ( TRUE , LKP_8.INDIV_PRTY_ID /* replaced lookup LKP_INDIV_CNT_MGR */ IS NOT NULL , LKP_9.INDIV_PRTY_ID /* replaced lookup LKP_INDIV_CNT_MGR */ , LKP_10.BUSN_PRTY_ID /* replaced lookup LKP_BUSN */ ) , UPPER ( SQ_pc_prty_rltd_distadjuster.Lookups ) = ''POLICY_INDIV'' , LKP_11.INDIV_PRTY_ID /* replaced lookup LKP_INDIV_CNT_MGR */ , UPPER ( SQ_pc_prty_rltd_distadjuster.Lookups ) = ''CLAIM_INDIV INTRNL_ORG'' , LKP_12.INDIV_PRTY_ID /* replaced lookup LKP_INDIV_CLM_CTR */ , UPPER ( SQ_pc_prty_rltd_distadjuster.Lookups ) = ''INTRNL_ORG'' , LKP_13.INTRNL_ORG_PRTY_ID /* replaced lookup LKP_INTRNL_ORG */ , UPPER ( SQ_pc_prty_rltd_distadjuster.Lookups ) = ''INTRNL_ORG POLICY_INDIV'' , LKP_14.INTRNL_ORG_PRTY_ID /* replaced lookup LKP_INTRNL_ORG */ , UPPER ( SQ_pc_prty_rltd_distadjuster.Lookups ) = ''INTRNL_ORG CLAIM_INDIV'' , LKP_15.INTRNL_ORG_PRTY_ID /* replaced lookup LKP_INTRNL_ORG */ ) as var_PRTY_ID,
CASE WHEN var_PRTY_ID IS NULL THEN 9999 ELSE var_PRTY_ID END as out_PRTY_ID,
DECODE ( TRUE , UPPER ( SQ_pc_prty_rltd_distadjuster.Lookups ) = ''BUSN POLICY_INDIV'' , LKP_16.BUSN_PRTY_ID /* replaced lookup LKP_BUSN */ , UPPER ( SQ_pc_prty_rltd_distadjuster.Lookups ) = ''POLICY_INDIV'' , LKP_17.INDIV_PRTY_ID /* replaced lookup LKP_INDIV_CNT_MGR */ , UPPER ( SQ_pc_prty_rltd_distadjuster.Lookups ) = ''INTRNL_ORG'' , LKP_18.INTRNL_ORG_PRTY_ID /* replaced lookup LKP_INTRNL_ORG */ , UPPER ( SQ_pc_prty_rltd_distadjuster.Lookups ) = ''CLAIM_INDIV INTRNL_ORG'' , LKP_19.INTRNL_ORG_PRTY_ID /* replaced lookup LKP_INTRNL_ORG */ , UPPER ( SQ_pc_prty_rltd_distadjuster.Lookups ) = ''INTRNL_ORG POLICY_INDIV'' , LKP_20.INDIV_PRTY_ID /* replaced lookup LKP_INDIV_CLM_CTR */ , UPPER ( SQ_pc_prty_rltd_distadjuster.Lookups ) = ''INTRNL_ORG CLAIM_INDIV'' , LKP_21.INDIV_PRTY_ID /* replaced lookup LKP_INDIV_CLM_CTR */ ) as var_RLTD_PRTY_ID,
CASE WHEN var_RLTD_PRTY_ID IS NULL THEN 9999 ELSE var_RLTD_PRTY_ID END as out_RLTD_PRTY_ID,
SQ_pc_prty_rltd_distadjuster.updatetime as in_PRTY_RLTD_STRT_DTTM,
SQ_pc_prty_rltd_distadjuster.ExpirationDate as PRTY_RLTD_END_DTTM,
DECODE ( TRUE , LKP_22.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_PRTY_STRC */ IS NULL , ''UNK'' , LKP_23.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_PRTY_STRC */ ) as PRTY_STRC_TYPE_CD,
:PRCS_ID as PRCS_ID,
CASE WHEN SQ_pc_prty_rltd_distadjuster.updatetime IS NULL THEN TO_TIMESTAMP(''01/01/1900'', ''MM/DD/YYYY'') ELSE SQ_pc_prty_rltd_distadjuster.updatetime END as out_Trans_strt_dttm,
SQ_pc_prty_rltd_distadjuster.EffectiveDate as EffectiveDate,
SQ_pc_prty_rltd_distadjuster.Retired as Retired,
SQ_pc_prty_rltd_distadjuster.source_record_id,
row_number() over (partition by SQ_pc_prty_rltd_distadjuster.source_record_id order by SQ_pc_prty_rltd_distadjuster.source_record_id) as RNK
FROM
SQ_pc_prty_rltd_distadjuster
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = SQ_pc_prty_rltd_distadjuster.Relationship
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = SQ_pc_prty_rltd_distadjuster.Relationship
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_INTRNL_ORG_SBTYPE LKP_3 ON LKP_3.SRC_IDNTFTN_VAL = SQ_pc_prty_rltd_distadjuster.Code
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_INTRNL_ORG_TYPE LKP_4 ON LKP_4.SRC_IDNTFTN_VAL = ''INTRNL_ORG_TYPE15''
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_INTRNL_ORG_SBTYPE LKP_5 ON LKP_5.SRC_IDNTFTN_VAL = SQ_pc_prty_rltd_distadjuster.PNI_AddressBookUID
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_INTRNL_ORG_SBTYPE LKP_6 ON LKP_6.SRC_IDNTFTN_VAL = SQ_pc_prty_rltd_distadjuster.Name
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_SRC_CD LKP_7 ON LKP_7.SRC_IDNTFTN_VAL = SQ_pc_prty_rltd_distadjuster.SYS_SRC_CD
LEFT JOIN LKP_INDIV_CNT_MGR LKP_8 ON LKP_8.NK_LINK_ID = SQ_pc_prty_rltd_distadjuster.PNI_AddressBookUID
LEFT JOIN LKP_INDIV_CNT_MGR LKP_9 ON LKP_9.NK_LINK_ID = SQ_pc_prty_rltd_distadjuster.PNI_AddressBookUID
LEFT JOIN LKP_BUSN LKP_10 ON LKP_10.BUSN_CTGY_CD = ''CO'' AND LKP_10.NK_BUSN_CD = SQ_pc_prty_rltd_distadjuster.PNI_AddressBookUID
LEFT JOIN LKP_INDIV_CNT_MGR LKP_11 ON LKP_11.NK_LINK_ID = SQ_pc_prty_rltd_distadjuster.PNI_AddressBookUID
LEFT JOIN LKP_INDIV_CLM_CTR LKP_12 ON LKP_12.NK_PUBLC_ID = SQ_pc_prty_rltd_distadjuster.PNI_AddressBookUID
LEFT JOIN LKP_INTRNL_ORG LKP_13 ON LKP_13.INTRNL_ORG_TYPE_CD = INTRNL_ORG_TYPE_CD AND LKP_13.INTRNL_ORG_SBTYPE_CD = INTRNL_ORG_SBTYPE_CD AND LKP_13.INTRNL_ORG_NUM = SQ_pc_prty_rltd_distadjuster.OTHER_AddressBookUID
LEFT JOIN LKP_INTRNL_ORG LKP_14 ON LKP_14.INTRNL_ORG_TYPE_CD = INTRNL_ORG_TYPE_CD AND LKP_14.INTRNL_ORG_SBTYPE_CD = INTRNL_ORG_SBTYPE_CD AND LKP_14.INTRNL_ORG_NUM = SQ_pc_prty_rltd_distadjuster.OTHER_AddressBookUID
LEFT JOIN LKP_INTRNL_ORG LKP_15 ON LKP_15.INTRNL_ORG_TYPE_CD = INTRNL_ORG_TYPE_CD AND LKP_15.INTRNL_ORG_SBTYPE_CD = INTRNL_ORG_SBTYPE_CD AND LKP_15.INTRNL_ORG_NUM = SQ_pc_prty_rltd_distadjuster.OTHER_AddressBookUID
LEFT JOIN LKP_BUSN LKP_16 ON LKP_16.BUSN_CTGY_CD = ''INSCAR'' AND LKP_16.NK_BUSN_CD = SQ_pc_prty_rltd_distadjuster.OTHER_AddressBookUID
LEFT JOIN LKP_INDIV_CNT_MGR LKP_17 ON LKP_17.NK_LINK_ID = SQ_pc_prty_rltd_distadjuster.OTHER_AddressBookUID
LEFT JOIN LKP_INTRNL_ORG LKP_18 ON LKP_18.INTRNL_ORG_TYPE_CD = ''INT'' AND LKP_18.INTRNL_ORG_SBTYPE_CD = UPPER ( var_Code ) AND LKP_18.INTRNL_ORG_NUM = UPPER ( SQ_pc_prty_rltd_distadjuster.Name )
LEFT JOIN LKP_INTRNL_ORG LKP_19 ON LKP_19.INTRNL_ORG_TYPE_CD = ''INT'' AND LKP_19.INTRNL_ORG_SBTYPE_CD = UPPER ( var_Name ) AND LKP_19.INTRNL_ORG_NUM = UPPER ( SQ_pc_prty_rltd_distadjuster.OTHER_AddressBookUID )
LEFT JOIN LKP_INDIV_CLM_CTR LKP_20 ON LKP_20.NK_PUBLC_ID = SQ_pc_prty_rltd_distadjuster.Code
LEFT JOIN LKP_INDIV_CLM_CTR LKP_21 ON LKP_21.NK_PUBLC_ID = SQ_pc_prty_rltd_distadjuster.Code
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_PRTY_STRC LKP_22 ON LKP_22.SRC_IDNTFTN_VAL = SQ_pc_prty_rltd_distadjuster.Party_Structure_Type_Cd
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_PRTY_STRC LKP_23 ON LKP_23.SRC_IDNTFTN_VAL = SQ_pc_prty_rltd_distadjuster.Party_Structure_Type_Cd
QUALIFY RNK = 1
);


-- Component LKP_TERADATA_ETL_REF_XLAT_RELATIONSHIP11, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_RELATIONSHIP11 AS
(
SELECT
LKP.TGT_IDNTFTN_VAL,
exp_all_source11.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_all_source11.source_record_id ORDER BY LKP.TGT_IDNTFTN_VAL desc,LKP.SRC_IDNTFTN_VAL desc) RNK
FROM
exp_all_source11
LEFT JOIN (
SELECT 
	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL
	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 
FROM 
	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT
WHERE 
	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM in ( ''PRTY_RLTD_ROLE'')
             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM in  (''derived'',''pc_role.name'') 
		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS in (''DS'',''GW'') 
		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
) LKP ON LKP.SRC_IDNTFTN_VAL = exp_all_source11.TYPECODE
QUALIFY RNK = 1
);


-- Component exp_SrcFields11, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_SrcFields11 AS
(
SELECT
exp_all_source11.out_PRTY_ID as in_PRTY_ID,
exp_all_source11.out_RLTD_PRTY_ID as in_RLTD_PRTY_ID,
LKP_TERADATA_ETL_REF_XLAT_RELATIONSHIP11.TGT_IDNTFTN_VAL as in_PRTY_RLTD_ROLE_CD,
exp_all_source11.in_PRTY_RLTD_STRT_DTTM as in_PRTY_RLTD_STRT_DTTM,
CASE WHEN exp_all_source11.PRTY_RLTD_END_DTTM IS NULL THEN TO_TIMESTAMP(''12/31/9999 23:59:59.999999'', ''MM/DD/YYYY HH24:MI:SS.NS'') ELSE exp_all_source11.PRTY_RLTD_END_DTTM END as o_PRTY_RLTD_END_DTTM,
exp_all_source11.PRTY_STRC_TYPE_CD as in_PRTY_STRC_TYPE_CD,
exp_all_source11.PRCS_ID as in_PRCS_ID,
exp_all_source11.out_Trans_strt_dttm as Trans_strt_dttm,
exp_all_source11.Retired as Retired,
CURRENT_TIMESTAMP as EDW_STRT_DTTM,
TO_TIMESTAMP(''12/31/9999 23:59:59.999999'', ''MM/DD/YYYY HH24:MI:SS.NS'') as EDW_END_DTTM,
DATEADD(''ss'', - 1, CURRENT_TIMESTAMP) as EDW_END_DTTM_exp,
exp_all_source11.EffectiveDate as EffectiveDate,
exp_all_source11.source_record_id
FROM
exp_all_source11
INNER JOIN LKP_TERADATA_ETL_REF_XLAT_RELATIONSHIP11 ON exp_all_source11.source_record_id = LKP_TERADATA_ETL_REF_XLAT_RELATIONSHIP11.source_record_id
);


-- Component LKP_PRTY_RLTD_CDC_DISTADJUSTER, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_PRTY_RLTD_CDC_DISTADJUSTER AS
(
SELECT
LKP.PRTY_ID,
LKP.RLTD_PRTY_ID,
LKP.PRTY_RLTD_ROLE_CD,
LKP.PRTY_RLTD_STRT_DTTM,
LKP.PRTY_RLTD_END_DTTM,
LKP.PRTY_STRC_TYPE_CD,
LKP.EDW_STRT_DTTM,
LKP.EDW_END_DTTM,
LKP.TRANS_STRT_DTTM,
exp_SrcFields11.in_PRTY_ID as in_PRTY_ID,
exp_SrcFields11.in_RLTD_PRTY_ID as in_RLTD_PRTY_ID,
exp_SrcFields11.in_PRTY_RLTD_ROLE_CD as in_PRTY_RLTD_ROLE_CD,
exp_SrcFields11.o_PRTY_RLTD_END_DTTM as o_PRTY_RLTD_END_DTTM,
exp_SrcFields11.in_PRTY_STRC_TYPE_CD as in_PRTY_STRC_TYPE_CD,
exp_SrcFields11.in_PRCS_ID as in_PRCS_ID,
exp_SrcFields11.Trans_strt_dttm as Trans_strt_dttm1,
exp_SrcFields11.EDW_STRT_DTTM as EDW_STRT_DTTM1,
exp_SrcFields11.EDW_END_DTTM as EDW_END_DTTM1,
exp_SrcFields11.EDW_END_DTTM_exp as EDW_END_DTTM_exp,
exp_SrcFields11.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_SrcFields11.source_record_id ORDER BY LKP.PRTY_ID asc,LKP.RLTD_PRTY_ID asc,LKP.PRTY_RLTD_ROLE_CD asc,LKP.PRTY_RLTD_STRT_DTTM asc,LKP.PRTY_RLTD_END_DTTM asc,LKP.PRTY_STRC_TYPE_CD asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc,LKP.TRANS_STRT_DTTM asc) RNK
FROM
exp_SrcFields11
LEFT JOIN (
SELECT PRTY_RLTD.PRTY_RLTD_STRT_DTTM as PRTY_RLTD_STRT_DTTM, PRTY_RLTD.PRTY_RLTD_END_DTTM as PRTY_RLTD_END_DTTM, PRTY_RLTD.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
PRTY_RLTD.PRTY_STRC_TYPE_CD as PRTY_STRC_TYPE_CD, 
PRTY_RLTD.EDW_STRT_DTTM as EDW_STRT_DTTM, PRTY_RLTD.EDW_END_DTTM as EDW_END_DTTM, PRTY_RLTD.PRTY_ID as PRTY_ID, PRTY_RLTD.RLTD_PRTY_ID as RLTD_PRTY_ID, PRTY_RLTD.PRTY_RLTD_ROLE_CD as PRTY_RLTD_ROLE_CD
  FROM DB_T_PROD_CORE.PRTY_RLTD WHERE PRTY_RLTD_ROLE_CD in (''DISTTOSPRVSR'',''DISTTOMGR'',''UWSTTODIST'',''RGNTODIST'',''STTORGN'',''DISTTOSVC'',''RGNTOGENL'',''DISTADJSTR'',''GENLTORGN'',''DISTTOSTRCEN'',''UWOFFCTOST'')
QUALIFY ROW_NUMBER() OVER(PARTITION BY RLTD_PRTY_ID,PRTY_RLTD_ROLE_CD ORDER BY EDW_END_DTTM desc) = 1
) LKP ON LKP.RLTD_PRTY_ID = exp_SrcFields11.in_RLTD_PRTY_ID AND LKP.PRTY_RLTD_ROLE_CD = exp_SrcFields11.in_PRTY_RLTD_ROLE_CD
QUALIFY RNK = 1
);


-- Component exp_CDC_Check11, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_CDC_Check11 AS
(
SELECT
LKP_PRTY_RLTD_CDC_DISTADJUSTER.in_PRTY_ID as in_PRTY_ID,
LKP_PRTY_RLTD_CDC_DISTADJUSTER.in_RLTD_PRTY_ID as in_RLTD_PRTY_ID,
LKP_PRTY_RLTD_CDC_DISTADJUSTER.in_PRTY_RLTD_ROLE_CD as in_PRTY_RLTD_ROLE_CD,
LKP_PRTY_RLTD_CDC_DISTADJUSTER.o_PRTY_RLTD_END_DTTM as in_PRTY_RLTD_END_DTTM,
LKP_PRTY_RLTD_CDC_DISTADJUSTER.in_PRTY_STRC_TYPE_CD as in_PRTY_STRC_TYPE_CD,
exp_SrcFields11.in_PRCS_ID as in_PRCS_ID,
LKP_PRTY_RLTD_CDC_DISTADJUSTER.PRTY_ID as lkp_PRTY_ID,
LKP_PRTY_RLTD_CDC_DISTADJUSTER.RLTD_PRTY_ID as lkp_RLTD_PRTY_ID,
LKP_PRTY_RLTD_CDC_DISTADJUSTER.PRTY_RLTD_ROLE_CD as lkp_PRTY_RLTD_ROLE_CD,
LKP_PRTY_RLTD_CDC_DISTADJUSTER.PRTY_RLTD_STRT_DTTM as lkp_PRTY_RLTD_STRT_DTTM,
LKP_PRTY_RLTD_CDC_DISTADJUSTER.PRTY_RLTD_END_DTTM as lkp_PRTY_RLTD_END_DTTM,
LKP_PRTY_RLTD_CDC_DISTADJUSTER.PRTY_STRC_TYPE_CD as lkp_PRTY_STRC_TYPE_CD,
NULL as lkp_PRCS_ID,
LKP_PRTY_RLTD_CDC_DISTADJUSTER.EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
LKP_PRTY_RLTD_CDC_DISTADJUSTER.EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_SrcFields11.EffectiveDate as EffectiveDate,
md5 ( to_char ( exp_SrcFields11.EffectiveDate ) || to_char ( LKP_PRTY_RLTD_CDC_DISTADJUSTER.o_PRTY_RLTD_END_DTTM ) || ltrim ( rtrim ( LKP_PRTY_RLTD_CDC_DISTADJUSTER.in_PRTY_STRC_TYPE_CD ) ) || TO_CHAR ( LKP_PRTY_RLTD_CDC_DISTADJUSTER.in_PRTY_ID ) ) as var_calc_chksm,
md5 ( ltrim ( rtrim ( to_char ( LKP_PRTY_RLTD_CDC_DISTADJUSTER.PRTY_RLTD_STRT_DTTM ) ) ) || ltrim ( rtrim ( to_char ( LKP_PRTY_RLTD_CDC_DISTADJUSTER.PRTY_RLTD_END_DTTM ) ) ) || ltrim ( rtrim ( LKP_PRTY_RLTD_CDC_DISTADJUSTER.PRTY_STRC_TYPE_CD ) ) || TO_CHAR ( LKP_PRTY_RLTD_CDC_DISTADJUSTER.PRTY_ID ) ) as var_orig_chksm,
CASE WHEN var_orig_chksm IS NULL THEN ''I'' ELSE CASE WHEN var_orig_chksm != var_calc_chksm THEN ''U'' ELSE ''R'' END END as o_flag_MD5,
LKP_PRTY_RLTD_CDC_DISTADJUSTER.EDW_STRT_DTTM1 as StartTime,
LKP_PRTY_RLTD_CDC_DISTADJUSTER.EDW_END_DTTM1 as EndTime,
LKP_PRTY_RLTD_CDC_DISTADJUSTER.Trans_strt_dttm1 as Trans_strt_dttm,
LKP_PRTY_RLTD_CDC_DISTADJUSTER.EDW_END_DTTM_exp as EDW_END_DTTM_exp,
exp_SrcFields11.Retired as Retired,
exp_SrcFields11.source_record_id
FROM
exp_SrcFields11
INNER JOIN LKP_PRTY_RLTD_CDC_DISTADJUSTER ON exp_SrcFields11.source_record_id = LKP_PRTY_RLTD_CDC_DISTADJUSTER.source_record_id
);


-- Component rtr_CDC_distadjuster_Insert, Type ROUTER Output Group Insert
CREATE OR REPLACE TEMPORARY TABLE rtr_CDC_distadjuster_Insert AS (
SELECT
exp_CDC_Check11.in_PRTY_ID as in_PRTY_ID,
exp_CDC_Check11.in_RLTD_PRTY_ID as in_RLTD_PRTY_ID,
exp_CDC_Check11.in_PRTY_RLTD_ROLE_CD as in_PRTY_RLTD_ROLE_CD,
NULL as in_PRTY_RLTD_STRT_DTTM,
exp_CDC_Check11.in_PRTY_RLTD_END_DTTM as in_PRTY_RLTD_END_DTTM,
exp_CDC_Check11.in_PRTY_STRC_TYPE_CD as in_PRTY_STRC_TYPE_CD,
exp_CDC_Check11.in_PRCS_ID as in_PRCS_ID,
exp_CDC_Check11.lkp_PRTY_ID as lkp_PRTY_ID,
exp_CDC_Check11.lkp_RLTD_PRTY_ID as lkp_RLTD_PRTY_ID,
exp_CDC_Check11.lkp_PRTY_RLTD_ROLE_CD as lkp_PRTY_RLTD_ROLE_CD,
exp_CDC_Check11.lkp_PRTY_RLTD_STRT_DTTM as lkp_PRTY_RLTD_STRT_DTTM,
exp_CDC_Check11.lkp_PRTY_RLTD_END_DTTM as lkp_PRTY_RLTD_END_DTTM,
exp_CDC_Check11.lkp_PRTY_STRC_TYPE_CD as lkp_PRTY_STRC_TYPE_CD,
exp_CDC_Check11.lkp_PRCS_ID as lkp_PRCS_ID,
exp_CDC_Check11.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_CDC_Check11.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
NULL as o_Src_Tgt,
exp_CDC_Check11.StartTime as StartTime,
exp_CDC_Check11.EndTime as EndTime,
exp_CDC_Check11.Trans_strt_dttm as Trans_strt_dttm,
exp_CDC_Check11.EDW_END_DTTM_exp as EDW_END_DTTM_exp,
exp_CDC_Check11.Retired as Retired,
exp_CDC_Check11.o_flag_MD5 as o_flag_MD5,
exp_CDC_Check11.EffectiveDate as EffectiveDate,
exp_CDC_Check11.source_record_id
FROM
exp_CDC_Check11
WHERE CASE WHEN exp_CDC_Check11.o_flag_MD5 = ''I'' AND exp_CDC_Check11.in_PRTY_ID != 9999 AND exp_CDC_Check11.in_RLTD_PRTY_ID != 9999 THEN 1 ELSE CASE WHEN exp_CDC_Check11.Retired = 0 AND exp_CDC_Check11.lkp_EDW_END_DTTM != TO_TIMESTAMP(''12/31/9999 23:59:59.999999'', ''MM/DD/YYYY HH24:MI:SS.NS'') THEN 1 ELSE 0 END END - - o_Src_Tgt = ''I'' OR ( exp_CDC_Check11.Retired = 0 AND exp_CDC_Check11.lkp_EDW_END_DTTM != TO_TIMESTAMP(''12/31/9999 23:59:59.999999'', ''MM/DD/YYYY HH24:MI:SS.NS'') and exp_CDC_Check11.lkp_PRTY_ID IS NOT NULL and exp_CDC_Check11.lkp_RLTD_PRTY_ID IS NOT NULL )
);


-- Component rtr_CDC_distadjuster_Retired, Type ROUTER Output Group Retired
CREATE OR REPLACE TEMPORARY TABLE rtr_CDC_distadjuster_Retired AS (
SELECT
exp_CDC_Check11.in_PRTY_ID as in_PRTY_ID,
exp_CDC_Check11.in_RLTD_PRTY_ID as in_RLTD_PRTY_ID,
exp_CDC_Check11.in_PRTY_RLTD_ROLE_CD as in_PRTY_RLTD_ROLE_CD,
NULL as in_PRTY_RLTD_STRT_DTTM,
exp_CDC_Check11.in_PRTY_RLTD_END_DTTM as in_PRTY_RLTD_END_DTTM,
exp_CDC_Check11.in_PRTY_STRC_TYPE_CD as in_PRTY_STRC_TYPE_CD,
exp_CDC_Check11.in_PRCS_ID as in_PRCS_ID,
exp_CDC_Check11.lkp_PRTY_ID as lkp_PRTY_ID,
exp_CDC_Check11.lkp_RLTD_PRTY_ID as lkp_RLTD_PRTY_ID,
exp_CDC_Check11.lkp_PRTY_RLTD_ROLE_CD as lkp_PRTY_RLTD_ROLE_CD,
exp_CDC_Check11.lkp_PRTY_RLTD_STRT_DTTM as lkp_PRTY_RLTD_STRT_DTTM,
exp_CDC_Check11.lkp_PRTY_RLTD_END_DTTM as lkp_PRTY_RLTD_END_DTTM,
exp_CDC_Check11.lkp_PRTY_STRC_TYPE_CD as lkp_PRTY_STRC_TYPE_CD,
exp_CDC_Check11.lkp_PRCS_ID as lkp_PRCS_ID,
exp_CDC_Check11.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_CDC_Check11.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
NULL as o_Src_Tgt,
exp_CDC_Check11.StartTime as StartTime,
exp_CDC_Check11.EndTime as EndTime,
exp_CDC_Check11.Trans_strt_dttm as Trans_strt_dttm,
exp_CDC_Check11.EDW_END_DTTM_exp as EDW_END_DTTM_exp,
exp_CDC_Check11.Retired as Retired,
exp_CDC_Check11.o_flag_MD5 as o_flag_MD5,
exp_CDC_Check11.EffectiveDate as EffectiveDate,
exp_CDC_Check11.source_record_id
FROM
exp_CDC_Check11
WHERE exp_CDC_Check11.o_flag_MD5 = ''R'' and exp_CDC_Check11.Retired != 0 and exp_CDC_Check11.lkp_EDW_END_DTTM = TO_TIMESTAMP(''12/31/9999 23:59:59.999999'', ''MM/DD/YYYY HH24:MI:SS.NS'') - - o_Src_Tgt = ''R'' and exp_CDC_Check11.Retired != 0 and exp_CDC_Check11.lkp_EDW_END_DTTM = TO_TIMESTAMP(''12/31/9999 23:59:59.999999'', ''MM/DD/YYYY HH24:MI:SS.NS'')
);


-- Component rtr_CDC_distadjuster_Update, Type ROUTER Output Group Update
CREATE OR REPLACE TEMPORARY TABLE rtr_CDC_distadjuster_Update AS (
SELECT
exp_CDC_Check11.in_PRTY_ID as in_PRTY_ID,
exp_CDC_Check11.in_RLTD_PRTY_ID as in_RLTD_PRTY_ID,
exp_CDC_Check11.in_PRTY_RLTD_ROLE_CD as in_PRTY_RLTD_ROLE_CD,
NULL as in_PRTY_RLTD_STRT_DTTM,
exp_CDC_Check11.in_PRTY_RLTD_END_DTTM as in_PRTY_RLTD_END_DTTM,
exp_CDC_Check11.in_PRTY_STRC_TYPE_CD as in_PRTY_STRC_TYPE_CD,
exp_CDC_Check11.in_PRCS_ID as in_PRCS_ID,
exp_CDC_Check11.lkp_PRTY_ID as lkp_PRTY_ID,
exp_CDC_Check11.lkp_RLTD_PRTY_ID as lkp_RLTD_PRTY_ID,
exp_CDC_Check11.lkp_PRTY_RLTD_ROLE_CD as lkp_PRTY_RLTD_ROLE_CD,
exp_CDC_Check11.lkp_PRTY_RLTD_STRT_DTTM as lkp_PRTY_RLTD_STRT_DTTM,
exp_CDC_Check11.lkp_PRTY_RLTD_END_DTTM as lkp_PRTY_RLTD_END_DTTM,
exp_CDC_Check11.lkp_PRTY_STRC_TYPE_CD as lkp_PRTY_STRC_TYPE_CD,
exp_CDC_Check11.lkp_PRCS_ID as lkp_PRCS_ID,
exp_CDC_Check11.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_CDC_Check11.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
NULL as o_Src_Tgt,
exp_CDC_Check11.StartTime as StartTime,
exp_CDC_Check11.EndTime as EndTime,
exp_CDC_Check11.Trans_strt_dttm as Trans_strt_dttm,
exp_CDC_Check11.EDW_END_DTTM_exp as EDW_END_DTTM_exp,
exp_CDC_Check11.Retired as Retired,
exp_CDC_Check11.o_flag_MD5 as o_flag_MD5,
exp_CDC_Check11.EffectiveDate as EffectiveDate,
exp_CDC_Check11.source_record_id
FROM
exp_CDC_Check11
WHERE exp_CDC_Check11.o_flag_MD5 = ''U'' AND exp_CDC_Check11.lkp_EDW_END_DTTM = TO_TIMESTAMP(''12/31/9999 23:59:59.999999'', ''MM/DD/YYYY HH24:MI:SS.NS'') - - o_Src_Tgt = ''U'' AND exp_CDC_Check11.lkp_EDW_END_DTTM = TO_TIMESTAMP(''12/31/9999 23:59:59.999999'', ''MM/DD/YYYY HH24:MI:SS.NS'') and exp_CDC_Check11.lkp_PRTY_ID IS NOT NULL and exp_CDC_Check11.lkp_RLTD_PRTY_ID IS NOT NULL
);


-- Component upd_Update11, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_Update11 AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_CDC_distadjuster_Update.lkp_PRTY_ID as lkp_PRTY_ID3,
rtr_CDC_distadjuster_Update.lkp_RLTD_PRTY_ID as lkp_RLTD_PRTY_ID3,
rtr_CDC_distadjuster_Update.lkp_PRTY_RLTD_ROLE_CD as lkp_PRTY_RLTD_ROLE_CD3,
rtr_CDC_distadjuster_Update.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM3,
rtr_CDC_distadjuster_Update.EDW_END_DTTM_exp as EDW_END_DTTM_exp3,
rtr_CDC_distadjuster_Update.lkp_PRTY_RLTD_STRT_DTTM as lkp_PRTY_RLTD_STRT_DTTM3,
rtr_CDC_distadjuster_Update.lkp_PRTY_RLTD_END_DTTM as lkp_PRTY_RLTD_END_DTTM3,
rtr_CDC_distadjuster_Update.lkp_PRTY_STRC_TYPE_CD as lkp_PRTY_STRC_TYPE_CD3,
rtr_CDC_distadjuster_Update.Trans_strt_dttm as Trans_strt_dttm3,
rtr_CDC_distadjuster_Update.source_record_id,
1 as UPDATE_STRATEGY_ACTION
FROM
rtr_CDC_distadjuster_Update
);


-- Component upd_insert11, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_insert11 AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_CDC_distadjuster_Insert.in_PRTY_ID as in_PRTY_ID1,
rtr_CDC_distadjuster_Insert.in_RLTD_PRTY_ID as in_RLTD_PRTY_ID1,
rtr_CDC_distadjuster_Insert.in_PRTY_RLTD_ROLE_CD as in_PRTY_RLTD_ROLE_CD1,
rtr_CDC_distadjuster_Insert.in_PRTY_RLTD_STRT_DTTM as in_PRTY_RLTD_STRT_DTTM1,
rtr_CDC_distadjuster_Insert.in_PRTY_RLTD_END_DTTM as in_PRTY_RLTD_END_DTTM1,
rtr_CDC_distadjuster_Insert.in_PRTY_STRC_TYPE_CD as in_PRTY_STRC_TYPE_CD1,
rtr_CDC_distadjuster_Insert.in_PRCS_ID as in_PRCS_ID1,
rtr_CDC_distadjuster_Insert.StartTime as StartTime1,
rtr_CDC_distadjuster_Insert.EndTime as EndTime1,
rtr_CDC_distadjuster_Insert.Trans_strt_dttm as Trans_strt_dttm1,
rtr_CDC_distadjuster_Insert.Retired as Retired1,
rtr_CDC_distadjuster_Insert.EffectiveDate as EffectiveDate1,
rtr_CDC_distadjuster_Insert.source_record_id,
0 as UPDATE_STRATEGY_ACTION
FROM
rtr_CDC_distadjuster_Insert
);


-- Component upd_Upd_retired11, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_Upd_retired11 AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_CDC_distadjuster_Retired.lkp_PRTY_ID as lkp_PRTY_ID3,
rtr_CDC_distadjuster_Retired.lkp_RLTD_PRTY_ID as lkp_RLTD_PRTY_ID3,
rtr_CDC_distadjuster_Retired.lkp_PRTY_RLTD_ROLE_CD as lkp_PRTY_RLTD_ROLE_CD3,
rtr_CDC_distadjuster_Retired.lkp_PRTY_RLTD_STRT_DTTM as lkp_PRTY_RLTD_STRT_DTTM3,
rtr_CDC_distadjuster_Retired.lkp_PRTY_RLTD_END_DTTM as lkp_PRTY_RLTD_END_DTTM3,
NULL as lkp_PRTY_STRC_TYPE_CD3,
rtr_CDC_distadjuster_Retired.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM3,
NULL as EDW_END_DTTM_exp3,
rtr_CDC_distadjuster_Retired.Trans_strt_dttm as Trans_strt_dttm4,
rtr_CDC_distadjuster_Retired.source_record_id,
1 as UPDATE_STRATEGY_ACTION
FROM
rtr_CDC_distadjuster_Retired
);


-- Component exp_prty_rltd_insert11, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_prty_rltd_insert11 AS
(
SELECT
upd_insert11.in_PRTY_ID1 as in_PRTY_ID1,
upd_insert11.in_RLTD_PRTY_ID1 as in_RLTD_PRTY_ID1,
upd_insert11.in_PRTY_RLTD_ROLE_CD1 as in_PRTY_RLTD_ROLE_CD1,
upd_insert11.in_PRTY_RLTD_END_DTTM1 as in_PRTY_RLTD_END_DTTM1,
upd_insert11.in_PRTY_STRC_TYPE_CD1 as in_PRTY_STRC_TYPE_CD1,
upd_insert11.in_PRCS_ID1 as in_PRCS_ID1,
upd_insert11.StartTime1 as StartTime1,
upd_insert11.Trans_strt_dttm1 as Trans_strt_dttm1,
CASE WHEN upd_insert11.Retired1 = 0 THEN TO_TIMESTAMP(''9999-12-31 23:59:59.999999'', ''YYYY-MM-DD HH24:MI:SS.NS'') ELSE CURRENT_TIMESTAMP END as out_EDW_END_DTTM,
CASE WHEN upd_insert11.Retired1 != 0 THEN upd_insert11.Trans_strt_dttm1 ELSE TO_TIMESTAMP(''9999-12-31 23:59:59.999999'', ''YYYY-MM-DD HH24:MI:SS.NS'') END as TRANS_END_DTTM,
upd_insert11.EffectiveDate1 as EffectiveDate1,
upd_insert11.source_record_id
FROM
upd_insert11
);


-- Component upd_Upd_ins11, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_Upd_ins11 AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_CDC_distadjuster_Update.in_PRTY_ID as PRTY_ID3,
rtr_CDC_distadjuster_Update.in_RLTD_PRTY_ID as RLTD_PRTY_ID3,
rtr_CDC_distadjuster_Update.in_PRTY_RLTD_ROLE_CD as PRTY_RLTD_ROLE_CD3,
rtr_CDC_distadjuster_Update.in_PRTY_RLTD_STRT_DTTM as in_PRTY_RLTD_STRT_DTTM3,
NULL as lkp_PRTY_RLTD_STRT_DTTM3,
NULL as lkp_PRTY_RLTD_END_DTTM3,
NULL as lkp_PRTY_STRC_TYPE_CD3,
rtr_CDC_distadjuster_Update.in_PRTY_STRC_TYPE_CD as in_PRTY_STRC_TYPE_CD3,
rtr_CDC_distadjuster_Update.in_PRTY_RLTD_END_DTTM as in_PRTY_RLTD_END_DTTM3,
rtr_CDC_distadjuster_Update.in_PRCS_ID as in_PRCS_ID3,
rtr_CDC_distadjuster_Update.StartTime as StartTime3,
rtr_CDC_distadjuster_Update.EndTime as EndTime3,
rtr_CDC_distadjuster_Update.Trans_strt_dttm as Trans_strt_dttm3,
rtr_CDC_distadjuster_Update.Retired as Retired3,
rtr_CDC_distadjuster_Update.EffectiveDate as EffectiveDate3,
rtr_CDC_distadjuster_Update.source_record_id,
0 as UPDATE_STRATEGY_ACTION
FROM
rtr_CDC_distadjuster_Update
);


-- Component FILTRANS11, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE FILTRANS11 AS
(
SELECT
upd_Upd_ins11.PRTY_ID3 as PRTY_ID3,
upd_Upd_ins11.RLTD_PRTY_ID3 as RLTD_PRTY_ID3,
upd_Upd_ins11.PRTY_RLTD_ROLE_CD3 as PRTY_RLTD_ROLE_CD3,
upd_Upd_ins11.in_PRTY_STRC_TYPE_CD3 as in_PRTY_STRC_TYPE_CD3,
upd_Upd_ins11.in_PRTY_RLTD_STRT_DTTM3 as in_PRTY_RLTD_STRT_DTTM3,
upd_Upd_ins11.in_PRTY_RLTD_END_DTTM3 as in_PRTY_RLTD_END_DTTM3,
upd_Upd_ins11.in_PRCS_ID3 as in_PRCS_ID3,
upd_Upd_ins11.StartTime3 as StartTime3,
upd_Upd_ins11.EndTime3 as EndTime3,
upd_Upd_ins11.Trans_strt_dttm3 as Trans_strt_dttm3,
upd_Upd_ins11.Retired3 as Retired3,
upd_Upd_ins11.EffectiveDate3 as EffectiveDate3,
upd_Upd_ins11.source_record_id
FROM
upd_Upd_ins11
WHERE upd_Upd_ins11.Retired3 = 0
);


-- Component EXPTRANS11, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXPTRANS11 AS
(
SELECT
upd_Update11.lkp_PRTY_ID3 as lkp_PRTY_ID3,
upd_Update11.lkp_RLTD_PRTY_ID3 as lkp_RLTD_PRTY_ID3,
upd_Update11.lkp_PRTY_RLTD_ROLE_CD3 as lkp_PRTY_RLTD_ROLE_CD3,
upd_Update11.lkp_EDW_STRT_DTTM3 as lkp_EDW_STRT_DTTM3,
upd_Update11.EDW_END_DTTM_exp3 as Expiring_EndDate,
DATEADD(SECOND, - 1, upd_Update11.Trans_strt_dttm3) as Trans_strt_dttm31,
upd_Update11.source_record_id
FROM
upd_Update11
);


-- Component EXPTRANS_upd_retired11, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXPTRANS_upd_retired11 AS
(
SELECT
upd_Upd_retired11.lkp_PRTY_ID3 as lkp_PRTY_ID3,
upd_Upd_retired11.lkp_RLTD_PRTY_ID3 as lkp_RLTD_PRTY_ID3,
upd_Upd_retired11.lkp_PRTY_RLTD_ROLE_CD3 as lkp_PRTY_RLTD_ROLE_CD3,
upd_Upd_retired11.lkp_EDW_STRT_DTTM3 as lkp_EDW_STRT_DTTM3,
CURRENT_TIMESTAMP as out_EDW_END_DTTM,
upd_Upd_retired11.Trans_strt_dttm4 as Trans_strt_dttm4,
upd_Upd_retired11.source_record_id
FROM
upd_Upd_retired11
);


-- Component EXPTRANS_UPD_INS11, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXPTRANS_UPD_INS11 AS
(
SELECT
FILTRANS11.PRTY_ID3 as PRTY_ID3,
FILTRANS11.RLTD_PRTY_ID3 as RLTD_PRTY_ID3,
FILTRANS11.PRTY_RLTD_ROLE_CD3 as PRTY_RLTD_ROLE_CD3,
FILTRANS11.in_PRTY_STRC_TYPE_CD3 as in_PRTY_STRC_TYPE_CD3,
FILTRANS11.in_PRTY_RLTD_END_DTTM3 as in_PRTY_RLTD_END_DTTM3,
FILTRANS11.in_PRCS_ID3 as in_PRCS_ID3,
FILTRANS11.StartTime3 as StartTime3,
FILTRANS11.EndTime3 as EndTime3,
FILTRANS11.Trans_strt_dttm3 as Trans_strt_dttm3,
FILTRANS11.EffectiveDate3 as EffectiveDate3,
FILTRANS11.source_record_id
FROM
FILTRANS11
);


-- Component tgt_prty_rltd_NewInsert_distadjuster, Type TARGET 
INSERT INTO DB_T_PROD_CORE.PRTY_RLTD
(
PRTY_ID,
RLTD_PRTY_ID,
PRTY_RLTD_ROLE_CD,
PRTY_RLTD_STRT_DTTM,
PRTY_RLTD_END_DTTM,
PRTY_STRC_TYPE_CD,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM,
TRANS_END_DTTM
)
SELECT
exp_prty_rltd_insert11.in_PRTY_ID1 as PRTY_ID,
exp_prty_rltd_insert11.in_RLTD_PRTY_ID1 as RLTD_PRTY_ID,
exp_prty_rltd_insert11.in_PRTY_RLTD_ROLE_CD1 as PRTY_RLTD_ROLE_CD,
exp_prty_rltd_insert11.EffectiveDate1 as PRTY_RLTD_STRT_DTTM,
exp_prty_rltd_insert11.in_PRTY_RLTD_END_DTTM1 as PRTY_RLTD_END_DTTM,
exp_prty_rltd_insert11.in_PRTY_STRC_TYPE_CD1 as PRTY_STRC_TYPE_CD,
exp_prty_rltd_insert11.in_PRCS_ID1 as PRCS_ID,
exp_prty_rltd_insert11.StartTime1 as EDW_STRT_DTTM,
exp_prty_rltd_insert11.out_EDW_END_DTTM as EDW_END_DTTM,
exp_prty_rltd_insert11.Trans_strt_dttm1 as TRANS_STRT_DTTM,
exp_prty_rltd_insert11.TRANS_END_DTTM as TRANS_END_DTTM
FROM
exp_prty_rltd_insert11;


-- Component tgt_prty_rltd_Update_distadjuster, Type TARGET 
MERGE INTO DB_T_PROD_CORE.PRTY_RLTD
USING EXPTRANS11 ON (PRTY_RLTD.PRTY_ID = EXPTRANS11.lkp_PRTY_ID3 AND PRTY_RLTD.RLTD_PRTY_ID = EXPTRANS11.lkp_RLTD_PRTY_ID3 AND PRTY_RLTD.PRTY_RLTD_ROLE_CD = EXPTRANS11.lkp_PRTY_RLTD_ROLE_CD3 AND PRTY_RLTD.EDW_STRT_DTTM = EXPTRANS11.lkp_EDW_STRT_DTTM3)
WHEN MATCHED THEN UPDATE
SET
PRTY_ID = EXPTRANS11.lkp_PRTY_ID3,
RLTD_PRTY_ID = EXPTRANS11.lkp_RLTD_PRTY_ID3,
PRTY_RLTD_ROLE_CD = EXPTRANS11.lkp_PRTY_RLTD_ROLE_CD3,
EDW_STRT_DTTM = EXPTRANS11.lkp_EDW_STRT_DTTM3,
EDW_END_DTTM = EXPTRANS11.Expiring_EndDate,
TRANS_END_DTTM = EXPTRANS11.Trans_strt_dttm31;


-- Component tgt_prty_rltd_Upd_retired_distadjuster, Type TARGET 
MERGE INTO DB_T_PROD_CORE.PRTY_RLTD
USING EXPTRANS_upd_retired11 ON (PRTY_RLTD.PRTY_ID = EXPTRANS_upd_retired11.lkp_PRTY_ID3 AND PRTY_RLTD.RLTD_PRTY_ID = EXPTRANS_upd_retired11.lkp_RLTD_PRTY_ID3 AND PRTY_RLTD.PRTY_RLTD_ROLE_CD = EXPTRANS_upd_retired11.lkp_PRTY_RLTD_ROLE_CD3 AND PRTY_RLTD.EDW_STRT_DTTM = EXPTRANS_upd_retired11.lkp_EDW_STRT_DTTM3)
WHEN MATCHED THEN UPDATE
SET
PRTY_ID = EXPTRANS_upd_retired11.lkp_PRTY_ID3,
RLTD_PRTY_ID = EXPTRANS_upd_retired11.lkp_RLTD_PRTY_ID3,
PRTY_RLTD_ROLE_CD = EXPTRANS_upd_retired11.lkp_PRTY_RLTD_ROLE_CD3,
EDW_STRT_DTTM = EXPTRANS_upd_retired11.lkp_EDW_STRT_DTTM3,
EDW_END_DTTM = EXPTRANS_upd_retired11.out_EDW_END_DTTM,
TRANS_END_DTTM = EXPTRANS_upd_retired11.Trans_strt_dttm4;


-- Component tgt_prty_rltd_Upd_Insert_distadjuster, Type TARGET 
INSERT INTO DB_T_PROD_CORE.PRTY_RLTD
(
PRTY_ID,
RLTD_PRTY_ID,
PRTY_RLTD_ROLE_CD,
PRTY_RLTD_STRT_DTTM,
PRTY_RLTD_END_DTTM,
PRTY_STRC_TYPE_CD,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM
)
SELECT
EXPTRANS_UPD_INS11.PRTY_ID3 as PRTY_ID,
EXPTRANS_UPD_INS11.RLTD_PRTY_ID3 as RLTD_PRTY_ID,
EXPTRANS_UPD_INS11.PRTY_RLTD_ROLE_CD3 as PRTY_RLTD_ROLE_CD,
EXPTRANS_UPD_INS11.EffectiveDate3 as PRTY_RLTD_STRT_DTTM,
EXPTRANS_UPD_INS11.in_PRTY_RLTD_END_DTTM3 as PRTY_RLTD_END_DTTM,
EXPTRANS_UPD_INS11.in_PRTY_STRC_TYPE_CD3 as PRTY_STRC_TYPE_CD,
EXPTRANS_UPD_INS11.in_PRCS_ID3 as PRCS_ID,
EXPTRANS_UPD_INS11.StartTime3 as EDW_STRT_DTTM,
EXPTRANS_UPD_INS11.EndTime3 as EDW_END_DTTM,
EXPTRANS_UPD_INS11.Trans_strt_dttm3 as TRANS_STRT_DTTM
FROM
EXPTRANS_UPD_INS11;


-- PIPELINE END FOR 2

-- PIPELINE START FOR 3

-- Component SQ_pc_prty_rltd_x_PRINS, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_pc_prty_rltd_x_PRINS AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as Relationship,
$2 as PNI_AddressBookUID,
$3 as OTHER_AddressBookUID,
$4 as Code,
$5 as Name,
$6 as EffectiveDate,
$7 as ExpirationDate,
$8 as Lookups,
$9 as SYS_SRC_CD,
$10 as updatetime,
$11 as Retired,
$12 as Party_Structure_Type_Cd,
$13 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT DISTINCT

	Relationship as Relationship, 

	PNI_AddressBookUID as PNI_AddressBookUID, 

	OTHER_AddressBookUID as OTHER_AddressBookUID, 

	Code as Code, 

	Name1 as Name1, 

	case when eff_dt is null then cast(''01/01/1900'' as date) else cast(eff_dt as date) end as EffectiveDate, 

	 case when end_dt is null then cast(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6)) else end_dt end as ExpirationDate, 

	 Lookups as Lookups,

	''SRC_SYS4'' AS SYS_SRC_CD,

	updatetime,

	 Retired,Party_Structure_Type_Cd from (

SELECT DISTINCT 

cast(case when cct.TYPECODE_stg=''district_alfa'' and childtype.TYPECODE_stg=''servicecenter_alfa'' then ''PRTY_RLTD_ROLE24''

when cct.TYPECODE_stg=''district_alfa'' and childtype.TYPECODE_stg=''stormcenter_alfa'' then ''PRTY_RLTD_ROLE41''

when cct.TYPECODE_stg=''general'' and childtype.TYPECODE_stg=''general'' then ''PRTY_RLTD_ROLE42''

when cct.TYPECODE_stg=''general'' and childtype.TYPECODE_stg=''medical_mgmt'' then ''PRTY_RLTD_ROLE44''

when cct.TYPECODE_stg=''general'' and childtype.TYPECODE_stg=''region_alfa'' then ''PRTY_RLTD_ROLE43''

when cct.TYPECODE_stg=''region_alfa'' and childtype.TYPECODE_stg=''district_alfa'' then ''PRTY_RLTD_ROLE23''

when cct.TYPECODE_stg=''region_alfa'' and childtype.TYPECODE_stg=''general'' then ''PRTY_RLTD_ROLE45''

when cct.TYPECODE_stg=''root'' and childtype.TYPECODE_stg=''general'' then ''PRTY_RLTD_ROLE46''

when cct.TYPECODE_stg=''district_alfa'' and childtype.TYPECODE_stg=''general'' then ''PRTY_RLTD_ROLE47''

else  NULL end as varchar(50)) as Relationship,

/* ''PRTY_RLTD_ROLE4'' as Relationship, -- Need to add value in XLAT table */
cast(cct.TYPECODE_stg as varchar(100))AS PNI_AddressBookUID,

cast(ccg.name_stg as varchar(100))AS OTHER_AddressBookUID,

cast(childtype.TYPECODE_stg as varchar(100)) AS Code,

child.name_stg AS Name1,

CAST(''1990-01-01 00:00:00.000000'' AS timestamp(6)) AS eff_dt, /*  Need to follow up with Ahmed			 */
CAST(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS end_dt, /*  Need to follow up with Ahmed		 */
cast(current_timestamp as timestamp(6))  AS UpdateTime,

cast(''INTRNL_ORG'' as varchar(50))as Lookups,  

 case when ccg.retired_stg=0 and child.retired_stg=0 and cct.retired_stg=0 and childtype.retired_stg=0 then 0 else 1 end as Retired,

cast(''PRTY_STRC_TYPE15'' as varchar(50)) AS Party_Structure_Type_Cd

FROM

DB_T_PROD_STAG.cc_parentgroup ccp inner join DB_T_PROD_STAG.cc_group ccg on ccg.id_stg=ccp.ForeignEntityID_stg

inner join DB_T_PROD_STAG.cctl_grouptype cct on cct.id_stg=ccg.GroupType_stg

inner join DB_T_PROD_STAG.cc_group child on child.id_stg=ccp.OwnerID_stg

inner join DB_T_PROD_STAG.cctl_grouptype childtype on childtype.id_stg=child.GroupType_stg

where not (cct.TYPECODE_stg = ''region_alfa'' and childtype.TYPECODE_stg = ''servicecenter_alfa'')

 union

/**District or Storm Center associated with the adjuster relationship****/

SELECT DISTINCT

	''PRTY_RLTD_ROLE12'' AS Relationship, 

	cctl_grouptypecode,

	cast(cc_group_name as varchar(100))AS PNI_AddressBookUID,

	cast(cc_contact_publicID as varchar(100))AS OTHER_AddressBookUID,

	cast(NULL as varchar(30)) AS Name1,

	CAST(''1900-01-01 00:00:00.000000'' AS timestamp(6)) AS eff_dt, 

	CAST(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS END_dt, 

	CAST(''1900-01-01 00:00:00.000000'' AS timestamp(6)) AS UpdateTime,

	''INTRNL_ORG Claim_INDIV'' AS Lookups, 

	CASE WHEN usersgroup_retired=0 and cc_contact_retired=0  then 0 else 1 END AS Retired,

	''PRTY_STRC_TYPE9''  AS Party_Structure_Type_Cd

	FROM 

	(SELECT 

			cgu.UserID_stg AS adjusterID,

			cct.TYPECODE_stg AS cctl_grouptypecode,

			cast(cg.Name_stg as varchar(100))AS cc_group_name,

			cc.PublicID_stg AS cc_contact_publicID,

			cg.retired_stg AS usersgroup_retired,

			cc.retired_stg AS cc_contact_retired,

			rank() over (Partition by cu.AdjusterCode_Alfa_stg order by CASE WHEN left(cu.adjusterCode_alfa_stg,1)=  CASE WHEN cct.Name_stg = ''District'' then left(cg.name_stg,1)

 			END or right(cu.adjusterCode_alfa_stg,1)= CASE WHEN cct.Name_stg = ''District'' then left(cg.name_stg,1)

END  then 1 else 2 END) AS rankDistrict, /* EIM-49995 */
 			adjustercode_alfa_stg

 from DB_T_PROD_STAG.cc_user cu

 INNER join DB_T_PROD_STAG.cc_contact cc ON cc.id_stg=cu.ContactID_stg

 LEFT join DB_T_PROD_STAG.cc_groupuser cgu on cgu.UserID_stg = cu.id_stg 

			LEFT join DB_T_PROD_STAG.cc_group cg ON cg.id_stg=cgu.GroupID_stg

			LEFT join DB_T_PROD_STAG.cctl_grouptype cct  ON cct.id_stg =cg.GroupType_stg

			WHERE cct.Name_stg in (''District'')

		)adjusterDistricts where  rankDistrict=1

 and adjustercode_alfa_stg is not null



union



/* Claim Hierarchy District/regional Manager */

SELECT

''PRTY_RLTD_ROLE25''  as Relationship, /*  Need to add value in XLAT table for PRTY_RLTD_ROLE */
cct.TYPECODE_stg,

cg.name_stg as PNI_AddressBookUID, /*  Can load value in this way as GRoup. */
cc.PublicID_stg AS OTHER_AddressBookUID,

cast(NULL as varchar(30)) AS Name1,

cg.createtime_stg as EffectiveDate,

cast(NULL as timestamp(6)) as ExpirationDate,

cc.UpdateTime_stg AS Updatetime,

''INTRNL_ORG Claim_INDIV'' as Lookups, 

 case when cu.retired_stg=0 and cg.retired_stg=0 and cc.retired_stg=0 and cct.retired_stg=0 then 0 else 1 end as Retired,

''PRTY_STRC_TYPE5'' AS Party_Structure_Type_Cd

from DB_T_PROD_STAG.cc_group cg 

 inner join DB_T_PROD_STAG.cc_groupuser cgu on cgu.groupid_stg = cg.id_stg

inner join DB_T_PROD_STAG.cctl_grouptype cct on cct.id_stg=cg.GroupType_stg

inner join DB_T_PROD_STAG.cc_user cu on cu.id_stg=cgu.UserID_stg and cgu.Manager_stg=1

inner join DB_T_PROD_STAG.cc_contact cc on cc.id_stg=cu.ContactID_stg

where cct.TYPECODE_stg in (''region_alfa'')



union



SELECT

''PRTY_RLTD_ROLE27''  as Relationship, /*  Need to add value in XLAT table for PRTY_RLTD_ROLE */
cct.TYPECODE_stg,

cg.name_stg as PNI_AddressBookUID, /*  Can load value in this way as GRoup. */
cc.PublicID_stg AS OTHER_AddressBookUID,

cast(NULL as varchar(30)) AS Name1,

cg.createtime_stg as EffectiveDate,

cast(NULL as timestamp(6)) as ExpirationDate,

cg.UpdateTime_stg AS Updatetime,

''INTRNL_ORG Claim_INDIV'' as Lookups, 

 case when cu.retired_stg=0 and cg.retired_stg=0 and cc.retired_stg=0 and cct.retired_stg=0 then 0 else 1 end as Retired,

''PRTY_STRC_TYPE5'' AS Party_Structure_Type_Cd

from DB_T_PROD_STAG.cc_group cg 

 inner join DB_T_PROD_STAG.cc_groupuser cgu on cgu.groupid_stg= cg.id_stg

inner join DB_T_PROD_STAG.cctl_grouptype cct on cct.id_stg=cg.GroupType_stg

inner join DB_T_PROD_STAG.cc_user cu on cu.id_stg=cgu.UserID_stg and cgu.Manager_stg=1

inner join DB_T_PROD_STAG.cc_contact cc on cc.id_stg=cu.ContactID_stg

where cct.TYPECODE_stg in (''district_alfa'')



union



/* Claim Hierarchy District/regional Supervisor */

SELECT

''PRTY_RLTD_ROLE28''  as Relationship, /*  Need to add value in XLAT table for PRTY_RLTD_ROLE */
  cct.TYPECODE_stg,

cg.name_stg as PNI_AddressBookUID, /*  Can load value in this way as GRoup. */
  cc.PublicID_stg AS OTHER_AddressBookUID,

  cast(NULL as varchar(30)) AS Name1,

  cg.createtime_stg as effectivedate,

  cast(NULL as timestamp(6)) as ExpirationDate,

  cg.UpdateTime_stg AS Updatetime,

  ''INTRNL_ORG Claim_INDIV'' as Lookups, 

  case when cu.retired_stg=0 and cg.retired_stg=0 and cc.retired_stg=0 and cct.retired_stg=0 then 0 else 1 end as Retired,

''PRTY_STRC_TYPE5'' AS Party_Structure_Type_Cd

from DB_T_PROD_STAG.cc_group cg 

 inner join DB_T_PROD_STAG.cctl_grouptype cct on cct.id_stg=cg.GroupType_stg

inner join DB_T_PROD_STAG.cc_user cu on cu.id_stg=cg.SupervisorID_stg

inner join DB_T_PROD_STAG.cc_contact cc on cc.id_stg=cu.ContactID_stg

where cct.TYPECODE_stg in (''district_alfa'')

/* order by TYPECODE,cc_group.Name,cc_group.UpdateTime */


union



SELECT

''PRTY_RLTD_ROLE25''  as Relationship, /*  Need to add value in XLAT table for PRTY_RLTD_ROLE */
  cct.TYPECODE_stg,

cg.name_stg as PNI_AddressBookUID, /*  Can load value in this way as GRoup. */
  cc.PublicID_stg AS OTHER_AddressBookUID,

  cast(NULL as varchar(30)) AS Name1,

  cg.createtime_stg as effectivedate,

  cast(NULL as timestamp(6)) as ExpirationDate,

  cg.UpdateTime_stg AS Updatetime,

  ''INTRNL_ORG Claim_INDIV'' as Lookups, 

  case when cu.retired_stg=0 and cg.retired_stg=0 and cc.retired_stg=0 and cct.retired_stg=0 then 0 else 1 end as Retired,

''PRTY_STRC_TYPE5'' AS Party_Structure_Type_Cd

from DB_T_PROD_STAG.cc_group cg 

 inner join DB_T_PROD_STAG.cctl_grouptype cct on cct.id_stg=cg.GroupType_stg

inner join DB_T_PROD_STAG.cc_user cu on cu.id_stg=cg.SupervisorID_stg

inner join DB_T_PROD_STAG.cc_contact cc on cc.id_stg=cu.ContactID_stg

where cct.TYPECODE_stg in (''region_alfa'')





/*********************************** Claims Hierarchy END***********************************/



union



/* Agent to Producer Area */	



/* in doc its 			pc_userproducercode-- date not present****/												 
SELECT DISTINCT																

cast(''PRTY_RLTD_ROLE10'' as varchar(50)) as Relationship, 															

pc.PublicID_stg,/* -- Party ID from Individual										 */
pp.Code_stg,/* --- Related Party ID from Internal Org						 */
cast(NULL as varchar(30)) AS Code,																

cast(''INTRNL_ORG_SBTYPE2'' as varchar(30)) AS Name1,																

CAST(''1990-01-01 00:00:00.000000'' AS timestamp(6)) AS eff_dt, /*  Need to follow up with Ahmed			 */
CAST(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS end_dt, /*  Need to follow up with Ahmed		 */
cast(current_timestamp as timestamp(6))  AS UpdateTime,																	

cast(''Claim_INDIV INTRNL_ORG'' as varchar(50)) as Lookups, 													

case when pp.retired_stg=0 and pc.retired_stg=0 and pcc.retired_stg=0 and pr.retired_stg=0 then 0 else 1 end as Retired,	

cast(''PRTY_STRC_TYPE16'' as varchar(50)) AS Structure 

from DB_T_PROD_STAG.pc_producercode	pp															

left outer join DB_T_PROD_STAG.pc_userproducercode pup on  pp.id_stg = pup.producercodeid_stg														

left outer join DB_T_PROD_STAG.pc_user pu on  pup.UserID_stg = pu.id_stg						

left outer join DB_T_PROD_STAG.pc_userrole pur on pu.id_stg = pur.UserID_stg									

left outer join DB_T_PROD_STAG.pc_role UserRole on pur.RoleID_stg = UserRole.ID_stg						

left outer join DB_T_PROD_STAG.pc_contact pc on  pu.ContactID_stg = pc.id_stg								

left outer join DB_T_PROD_STAG.pctl_contact pcc on  pc.Subtype_stg = pcc.ID_stg							

left outer join DB_T_PROD_STAG.pc_producercoderole ppr on  pp.id_stg = ppr.ProducerCodeID_stg															

left outer join DB_T_PROD_STAG.pc_role pr on  ppr.RoleID_stg = pr.ID_stg							

left outer join DB_T_PROD_STAG.pc_role ProducerCodeRole on  ppr.RoleID_stg = ProducerCodeRole.ID_stg																

where pcc.name_stg=''User Contact''															

AND pr.name_stg=''Agent''																

-- AND UserRole.name_stg = ''Agent''	-- EIM - 36268 */															 */
UNION																

/****Party related (Primary Named Insured and Prior Carrier******/								

SELECT DISTINCT																

''PRTY_RLTD_ROLE3'' as Relationship, /*  Need to get confirmation from Keitra			 */
pc.AddressBookUID_stg, /* Party ID from Policy_INDIVidual							 */
ppa.TYPECODE_stg, /* Related party ID from Business,						 */
	cast(NULL as varchar(30)) AS Code, 													

	cast(NULL as varchar(30)) AS Name1,															

CAST(''1990-01-01 00:00:00.000000'' AS timestamp(6)) AS eff_dt, /*  Need to follow up with Ahmed			 */
CAST(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS end_dt,/*  Need to follow up with Ahmed		 */
pp.updatetime_stg AS UpdateTime,															

	''BUSN Policy_INDIV'' as Lookups, 															

case when pc.retired_stg=0 and ppa.retired_stg=0 and pcc.retired_stg=0 and pp.retired_stg=0 and pj.retired_stg=0 then 0 else 1 end as Retired,																

cast(NULL as varchar(50)) AS Structure																

from DB_T_PROD_STAG.pc_policycontactrole ppc 															

	INNER JOIN DB_T_PROD_STAG.pc_contact pc ON pc.id_stg=ppc.ContactDenorm_stg															

	INNER JOIN DB_T_PROD_STAG.pctl_policycontactrole ppcp ON ppcp.id_stg=ppc.Subtype_stg															

	INNER JOIN DB_T_PROD_STAG.pctl_contact pcc ON pcc.id_stg=pc.Subtype_stg	 														

	INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp ON pp.id_stg=ppc.BranchID_stg															

	INNER JOIN DB_T_PROD_STAG.pc_effectivedatedfields pe ON pp.id_stg=pe.BranchID_stg															

	INNER JOIN DB_T_PROD_STAG.pc_job pj ON pj.id_stg=pp.JobID_stg															

	INNER JOIN DB_T_PROD_STAG.pctl_job pcj ON pcj.id_stg=pj.Subtype_stg															

	INNER JOIN DB_T_PROD_STAG.pctl_policyperiodstatus pps ON pps.id_stg=pp.Status_stg															

	INNER JOIN DB_T_PROD_STAG.pctl_priorcarrier_alfa ppa ON ppa.id_stg=pe.PriorCarrier_alfa_stg															

WHERE 																

	pps.TYPECODE_stg=''Bound'' AND 															

																

	ppcp.TYPECODE_stg=''PolicyPriNamedInsured''AND															

pe.ExpirationDate_stg IS NULL /* AND															 */
/* pc_effectivedatedfields.UpdateTime > 						 */


  AND (  ( ppc.UpdateTime_stg > (:start_dttm) and ppc.UpdateTime_stg <= (:end_dttm))

 or ( pc.UpdateTime_stg > (:start_dttm) and pc.UpdateTime_stg <= (:end_dttm))

 or ( pp.UpdateTime_stg > (:start_dttm) and pp.UpdateTime_stg <= (:end_dttm))

 or ( pj.UpdateTime_stg > (:start_dttm) and pj.UpdateTime_stg<= (:end_dttm))

 or ( pe.UpdateTime_stg > (:start_dttm) and pe.UpdateTime_stg <= (:end_dttm))  )									

union																											

/***********************************Sales Hierarchy Start***********************************/

/** Sales Hierarchy ALFA to State **/



-- in doc its 			pc_parentgroup--  date not present


/* Use INT ORG to get parent and child PARTY ID */
SELECT DISTINCT

''PRTY_RLTD_ROLE29'' as Relationship, /*  Need to add value in XLAT table */
pcg.TYPECODE_stg AS Subtype,

pg.name_stg as code,

''INTRNL_ORG_SBTYPE4'' AS Name1,

prz.code_stg AS Subtype,

CAST(''1990-01-01 00:00:00.000000'' AS timestamp(6)) AS eff_dt, /*  Need to follow up with Ahmed			 */
CAST(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS end_dt, /*  Need to follow up with Ahmed		 */
cast(current_timestamp as timestamp(6))  AS UpdateTime,		

''INTRNL_ORG'' as Lookups, 

 case when pg.retired_stg=0 and child.retired_stg=0 and district.retired_stg=0 then 0 else 1 end as Retired,

''PRTY_STRC_TYPE16'' AS Structure  /* SLSHRCHY */
FROM

/**************ALFA to MARKETING************/

DB_T_PROD_STAG.pc_parentgroup ppg INNER JOIN DB_T_PROD_STAG.pc_group pg on pg.id_stg=ppg.ForeignEntityID_stg

INNER JOIN DB_T_PROD_STAG.pc_group child on child.id_stg=ppg.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pctl_grouptype pcg on pcg.id_stg=pg.GroupType_stg

INNER JOIN DB_T_PROD_STAG.pctl_grouptype childgrouptype on childgrouptype.id_stg=child.GroupType_stg

/************Marketing to District************/

INNER JOIN DB_T_PROD_STAG.pc_parentgroup pg_marketing_district on pg_marketing_district.ForeignEntityID_stg=ppg.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pc_group district on district.id_stg=pg_marketing_district.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pctl_grouptype district_type on district_type.id_stg=district.GroupType_stg

/********District to State*****************/

INNER JOIN DB_T_PROD_STAG.pc_groupregion pgr on pgr.GroupID_stg=pg_marketing_district.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pc_region_zone prz on pgr.RegionID_stg=prz.RegionId_stg

where 

(pcg.TYPECODE_stg=''root'' and childgrouptype.TYPECODE_stg=''region'') 



UNION



/** Sales Hierarchy State to Marketing **/

/* Use INT ORG to get parent and child PARTY ID */
SELECT DISTINCT

''PRTY_RLTD_ROLE22'' as Relationship, /*  Need to add value in XLAT table */
  ''INTRNL_ORG_SBTYPE4'' AS Name1,

  prz.code_stg,

childgrouptype.TYPECODE_stg,

child.name_stg AS OTHER_AddressBookUID,

CAST(''1990-01-01 00:00:00.000000'' AS timestamp(6)) AS eff_dt, /*  Need to follow up with Ahmed			 */
CAST(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS end_dt, /*  Need to follow up with Ahmed		 */
cast(current_timestamp as timestamp(6))  AS UpdateTime,		

''INTRNL_ORG'' as Lookups, 

 case when pg.retired_stg=0 and child.retired_stg=0 and district.retired_stg=0 then 0 else 1 end as Retired,

''PRTY_STRC_TYPE16'' AS Structure  /* SLSHRCHY */
FROM

/**************ALFA to MARKETING************/

DB_T_PROD_STAG.pc_parentgroup ppg INNER JOIN DB_T_PROD_STAG.pc_group pg on pg.id_stg=ppg.ForeignEntityID_stg

INNER JOIN DB_T_PROD_STAG.pc_group child on child.id_stg=ppg.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pctl_grouptype pcg on pcg.id_stg=pg.GroupType_stg

INNER JOIN DB_T_PROD_STAG.pctl_grouptype childgrouptype on childgrouptype.id_stg=child.GroupType_stg

/************Marketing to District************/

INNER JOIN DB_T_PROD_STAG.pc_parentgroup pg_marketing_district on pg_marketing_district.ForeignEntityID_stg=ppg.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pc_group district on district.id_stg=pg_marketing_district.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pctl_grouptype district_type on district_type.id_stg=district.GroupType_stg

/********District to State*****************/

INNER JOIN DB_T_PROD_STAG.pc_groupregion pgr on pgr.GroupID_stg=pg_marketing_district.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pc_region_zone prz on pgr.RegionID_stg=prz.RegionId_stg

where 

(pcg.TYPECODE_stg=''root'' and childgrouptype.TYPECODE_stg=''region'') 

 union

/* All Supervisors*/

SELECT DISTINCT

case when childgrouptype.TYPECODE_stg=''custserv'' then ''PRTY_RLTD_ROLE53'' 

when childgrouptype.TYPECODE_stg=''homeofficeadmin'' then ''PRTY_RLTD_ROLE54''

when childgrouptype.TYPECODE_stg=''region'' then ''PRTY_RLTD_ROLE26''

when childgrouptype.TYPECODE_stg=''salesdistrict_alfa'' then ''PRTY_RLTD_ROLE28'' 

when childgrouptype.TYPECODE_stg=''servicecenter_alfa'' then ''PRTY_RLTD_ROLE31''

when childgrouptype.TYPECODE_stg=''underwritingdistrict_alfa'' then ''PRTY_RLTD_ROLE28''

when childgrouptype.TYPECODE_stg=''homeofficeuw'' then ''PRTY_RLTD_ROLE55''

else NULL end as Relationship,

childgrouptype.TYPECODE_stg,

child.name_stg AS PNI_AddressBookUID,

  pc.PublicID_stg AS OTHER_AddressBookUID,

  cast(NULL as varchar(30)) AS Name1,

CAST(''1990-01-01 00:00:00.000000'' AS timestamp(6)) AS eff_dt, /*  Need to follow up with Ahmed			 */
CAST(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS end_dt, /*  Need to follow up with Ahmed		 */
cast(current_timestamp as timestamp(6))  AS UpdateTime,		

''INTRNL_ORG Claim_INDIV'' as Lookups, 

 case when pg.retired_stg=0 and child.retired_stg=0 and pu.retired_stg=0 and pc.retired_stg=0 then 0 else 1 end as Retired,

case when childgrouptype.TYPECODE_stg in(''underwritingdistrict_alfa'',''homeofficeuw'') then ''PRTY_STRC_TYPE14''

else ''PRTY_STRC_TYPE17'' end as Structure  /* SLSPRSASSGN */
from DB_T_PROD_STAG.pc_parentgroup ppg INNER JOIN DB_T_PROD_STAG.pc_group pg on pg.id_stg=ppg.ForeignEntityID_stg

INNER JOIN DB_T_PROD_STAG.pc_group child on child.id_stg=ppg.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pc_user pu on pu.id_stg=child.SupervisorID_stg

INNER JOIN DB_T_PROD_STAG.pc_contact pc on pc.id_stg=pu.ContactID_stg

INNER JOIN DB_T_PROD_STAG.pctl_grouptype pcg on pcg.id_stg=pg.GroupType_stg

INNER JOIN DB_T_PROD_STAG.pctl_grouptype childgrouptype on childgrouptype.id_stg=child.GroupType_stg

/* where  */
/* (pctl_grouptype.TYPECODE=''root'' and childgrouptype.TYPECODE=''region'')  */
union

/* All Managers*/

SELECT DISTINCT

case when childgrouptype.TYPECODE_stg=''custserv'' then ''PRTY_RLTD_ROLE51'' 

when childgrouptype.TYPECODE_stg=''homeofficeadmin'' then ''PRTY_RLTD_ROLE52''

when childgrouptype.TYPECODE_stg=''region'' then ''PRTY_RLTD_ROLE25''

when childgrouptype.TYPECODE_stg=''salesdistrict_alfa'' then ''PRTY_RLTD_ROLE27''

when childgrouptype.TYPECODE_stg=''servicecenter_alfa'' then ''PRTY_RLTD_ROLE32''

when childgrouptype.TYPECODE_stg=''underwritingdistrict_alfa'' then ''PRTY_RLTD_ROLE27''

when childgrouptype.TYPECODE_stg=''homeofficeuw'' then ''PRTY_RLTD_ROLE56''

else NULL end as Relationship,

/* ''RGNTOMGR'' as Relationship1, -- Need to add value in XLAT table */
childgrouptype.TYPECODE_stg,

child.name_stg AS PNI_AddressBookUID,

  pc.PublicID_stg AS OTHER_AddressBookUID,

  cast(NULL as varchar(30)) AS Name1,

CAST(''1990-01-01 00:00:00.000000'' AS timestamp(6)) AS eff_dt, /*  Need to follow up with Ahmed			 */
CAST(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS end_dt, /*  Need to follow up with Ahmed		 */
cast(current_timestamp as timestamp(6))  AS UpdateTime,	

''INTRNL_ORG Claim_INDIV'' as Lookups, 

 case when pg.retired_stg=0 and child.retired_stg=0 and pu.retired_stg=0 and pc.retired_stg=0 then 0 else 1 end as Retired,

case when childgrouptype.TYPECODE_stg in(''underwritingdistrict_alfa'',''homeofficeuw'') then ''PRTY_STRC_TYPE14''

else ''PRTY_STRC_TYPE17'' end as Structure  /* SLSPRSASSGN */
from DB_T_PROD_STAG.pc_parentgroup ppg INNER JOIN DB_T_PROD_STAG.pc_group pg on pg.id_stg=ppg.ForeignEntityID_stg

INNER JOIN DB_T_PROD_STAG.pc_group child on child.id_stg=ppg.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pc_groupuser pgu on pgu.GroupID_stg=child.id_stg and pgu.Manager_stg=1

INNER JOIN DB_T_PROD_STAG.pc_user pu on pu.id_stg=pgu.UserID_stg 

 INNER JOIN DB_T_PROD_STAG.pc_contact pc on pc.id_stg=pu.ContactID_stg

INNER JOIN DB_T_PROD_STAG.pctl_grouptype pcg on pcg.id_stg=pg.GroupType_stg

INNER JOIN DB_T_PROD_STAG.pctl_grouptype childgrouptype on childgrouptype.id_stg=child.GroupType_stg



union



/* Region to District*/

SELECT DISTINCT

''PRTY_RLTD_ROLE23'' as Relationship, /*  Need to add value in XLAT table  */
 pcg.TYPECODE_stg,

pg.name_stg as PNI_AddressBookUID,

childgrouptype.TYPECODE_stg,

child.name_stg AS OTHER_AddressBookUID,

CAST(''1990-01-01 00:00:00.000000'' AS timestamp(6)) AS eff_dt, /*  Need to follow up with Ahmed			 */
CAST(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS end_dt, /*  Need to follow up with Ahmed		 */
cast(current_timestamp as timestamp(6))  AS UpdateTime,	

''INTRNL_ORG'' as Lookups, 

 case when pg.retired_stg=0 and child.retired_stg=0 then 0 else 1 end as Retired,

''PRTY_STRC_TYPE16'' AS Structure  /*  SLSHRCY */
from DB_T_PROD_STAG.pc_parentgroup ppg INNER JOIN DB_T_PROD_STAG.pc_group pg on pg.id_stg=ppg.ForeignEntityID_stg

INNER JOIN DB_T_PROD_STAG.pc_group child on child.id_stg=ppg.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pctl_grouptype pcg on pcg.id_stg=pg.GroupType_stg

  INNER JOIN DB_T_PROD_STAG.pctl_grouptype childgrouptype on childgrouptype.id_stg=child.GroupType_stg

where 

(pcg.TYPECODE_stg=''region'' and childgrouptype.TYPECODE_stg=''salesdistrict_alfa'') 

/* OR (pctl_grouptype.TYPECODE=''salesdistrict_alfa'' and childgrouptype.TYPECODE=''servicecenter_alfa'') */
union



/*District to SVC*/



SELECT DISTINCT

''PRTY_RLTD_ROLE24'' as Relationship, /*  Need to add value in XLAT table  */
 pcg.TYPECODE_stg,

pg.name_stg as PNI_AddressBookUID,

childgrouptype.TYPECODE_stg,

child.name_stg AS OTHER_AddressBookUID,

CAST(''1990-01-01 00:00:00.000000'' AS timestamp(6)) AS eff_dt, /*  Need to follow up with Ahmed			 */
CAST(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS end_dt, /*  Need to follow up with Ahmed		 */
cast(current_timestamp as timestamp(6))  AS UpdateTime,

''INTRNL_ORG'' as Lookups, 

 case when pg.retired_stg=0 and child.retired_stg=0 then 0 else 1 end as Retired,

''PRTY_STRC_TYPE16'' AS Structure  /*  SLSHRCY */
from DB_T_PROD_STAG.pc_parentgroup ppg INNER JOIN DB_T_PROD_STAG.pc_group pg on pg.id_stg=ppg.ForeignEntityID_stg

INNER JOIN DB_T_PROD_STAG.pc_group child on child.id_stg=ppg.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pctl_grouptype pcg on pcg.id_stg=pg.GroupType_stg

  INNER JOIN DB_T_PROD_STAG.pctl_grouptype childgrouptype on childgrouptype.id_stg=child.GroupType_stg

where 

/* (pctl_grouptype.TYPECODE=''region'' and childgrouptype.TYPECODE=''salesdistrict_alfa'') or */
(pcg.TYPECODE_stg=''salesdistrict_alfa'' and childgrouptype.TYPECODE_stg=''servicecenter_alfa'')

UNION



/*SELECT DISTINCT

pc_role.Name as Relationship, --  Need to add value in XLAT table 
''INTRNL_ORG_SBTYPE2'' AS Subtype,

pc_producercode.code AS PNI_AddressBookUID,

  pc_contact.PublicID AS OTHER_AddressBookUID,

  NULL AS Name,

convert(datetime, ''01/01/1990'', 101) AS eff_dt, -- Need to follow up with Ahmed			 
CAST(''9999-12-31 23:59:59.9999999'' AS datetime2(7)) AS end_dt, --  Need to follow up with Ahmed		 
 getdate() AS UpdateTime,	

''INTRNL_ORG Policy_INDIV'' as Lookups, 

 case when pc_producercode.retired=0 and pc_user.retired=0 and pc_contact.retired=0 then 0 else 1 end as Retired,

''PRTY_STRC_TYPE17'' AS Structure  -- --SLSPRSASSGN 
,(:start_dttm) as start_dttm,

(:end_dttm) as end_dttm

from DB_T_PROD_STAG.pc_producercode INNER JOIN DB_T_PROD_STAG.pc_userproducercode on pc_userproducercode.ProducerCodeID=pc_producercode.id

inner join DB_T_PROD_STAG.pc_user on pc_user.id=pc_userproducercode.UserID 

 INNER JOIN DB_T_PROD_STAG.pc_contact on pc_contact.id=pc_user.ContactID

INNER JOIN DB_T_PROD_STAG.pc_userrole on pc_userrole.UserID=pc_user.ID

INNER JOIN DB_T_PROD_STAG.pc_role on pc_role.id=pc_userrole.RoleID

where pc_role.Name in (''Agent'',''CSR'')*/



/****Adding as fix for EIM-13666- Ankit- 5/30/2017*********************/

/** SVC to Role **/

SELECT DISTINCT

i.Name_stg as Relationship, /*  Need to add value in XLAT table */
b.typecode_stg AS Subtype,

a.NameDenorm_stg AS PNI_AddressBookUID,

/* a.NameDenorm_stg collate  SQL_Latin1_General_CP1_CI_AS AS PNI_AddressBookUID, */
  e.PublicID_stg AS OTHER_AddressBookUID,

  cast(NULL as varchar(30)) AS Name1,

CAST(''1990-01-01 00:00:00.000000'' AS timestamp(6)) AS eff_dt, /*  Need to follow up with Ahmed			 */
CAST(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS end_dt, /*  Need to follow up with Ahmed		 */
cast(current_timestamp as timestamp(6))  AS UpdateTime,

''INTRNL_ORG Policy_INDIV'' as Lookups, 

 case when a.retired_stg=0 and d.retired_stg=0 and e.retired_stg=0 then 0 else 1 end as Retired,

''PRTY_STRC_TYPE17'' AS Structure  /* --SLSPRSASSGN */
from DB_T_PROD_STAG.pc_group a

INNER JOIN DB_T_PROD_STAG.pctl_grouptype b on b.id_stg = a.GroupType_stg

INNER JOIN DB_T_PROD_STAG.pc_groupuser c on c.GroupID_stg = a.id_stg

INNER JOIN DB_T_PROD_STAG.pc_user d on d.id_stg = c.UserID_stg

INNER JOIN DB_T_PROD_STAG.pc_contact e on e.id_stg = d.ContactID_stg 

INNER JOIN DB_T_PROD_STAG.pc_userrole h on h.UserID_stg = d.id_stg

INNER JOIN DB_T_PROD_STAG.pc_role i on i.id_stg = h.RoleID_stg

where b.typecode_stg = ''servicecenter_alfa''

/* and i.name_stg <> ''CRC Team Leaders'' */


UNION



/** Sales Hierarchy Producer to User **/

SELECT DISTINCT

''PRTY_RLTD_ROLE91'' as Relationship, /*  Need to add value in XLAT table */
''INTRNL_ORG_SBTYPE2'' AS Subtype,

 pp.code_stg AS PNI_AddressBookUID,

  pc.PublicID_stg AS OTHER_AddressBookUID,

  cast(NULL as varchar(30)) AS Name1,

CAST(''1990-01-01 00:00:00.000000'' AS timestamp(6)) AS eff_dt, /*  Need to follow up with Ahmed			 */
CAST(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS end_dt, /*  Need to follow up with Ahmed		 */
cast(current_timestamp as timestamp(6))  AS UpdateTime,	

''INTRNL_ORG Policy_INDIV'' as Lookups, 

 case when pp.retired_stg=0 and pu.retired_stg=0 and pc.retired_stg=0 then 0 else 1 end as Retired,

''PRTY_STRC_TYPE17'' AS Structure /* --SLSPRSASSGN */
from DB_T_PROD_STAG.pc_producercode pp INNER JOIN DB_T_PROD_STAG.pc_userproducercode pup on pup.ProducerCodeID_stg=pp.id_stg

inner join DB_T_PROD_STAG.pc_user pu on pu.id_stg=pup.UserID_stg

 INNER JOIN DB_T_PROD_STAG.pc_contact pc on pc.id_stg=pu.ContactID_stg



UNION



/* Producer Area to Service center */

SELECT Relationship,TYPECODE1,PNI_AddressBookUID,TYPECODE2,OTHER_AddressBookUID,eff_dt,end_dt,UpdateTime,Lookups,Retired,Structure from 

(

SELECT DISTINCT

''PRTY_RLTD_ROLE30'' AS Relationship, 

 ''INTRNL_ORG_SBTYPE2'' AS TYPECODE1,

cast(pp.Code_stg as varchar(100)) AS PNI_AddressBookUID, /* --- Party ID from Internal Org */
pcg.TYPECODE_stg AS TYPECODE2,

cast(pg.NameDenorm_stg as varchar(100)) AS OTHER_AddressBookUID,  /* --- Related Party ID from Internal Org    */
/* pc_group.NameDenorm_stg collate  SQL_Latin1_General_CP1_CI_AS AS OTHER_AddressBookUID, */
CAST(''1990-01-01 00:00:00.000000'' AS timestamp(6)) AS eff_dt, /*  Need to follow up with Ahmed           */
CAST(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS end_dt, /*  Need to follow up with Ahmed   */
 pg.UpdateTime_stg AS UpdateTime, 

 ''INTRNL_ORG'' as Lookups, 

 case when pp.retired_stg=0 then 0 else 1 end as Retired,  

''PRTY_STRC_TYPE16'' AS Structure,  /* --- SLSHRCY */
ROW_NUMBER() OVER (PARTITION BY pp.Code_stg  ORDER BY pg.UpdateTime_stg DESC, pg.id_stg DESC ) AS RN /*  added pg.id_stg for DP-14182 */
from DB_T_PROD_STAG.pc_group pg 

 join DB_T_PROD_STAG.pc_groupuser pgu on pg.ID_stg=pgu.GroupID_stg

join DB_T_PROD_STAG.pc_user pu on pgu.UserID_stg=pu.ID_stg

join DB_T_PROD_STAG.pc_contact pc on pu.ContactID_stg=pc.ID_stg

join DB_T_PROD_STAG.pctl_grouptype pcg on pg.GroupType_stg=pcg.ID_stg

--  join DB_T_PROD_STAG.pctl_usertype pcu on pcu.ID_stg=pu.UserType_stg --EIM -35544*/ 
LEFT join DB_T_PROD_STAG.pc_userproducercode pup on pu.ID_stg=pup.UserID_stg

LEFT join DB_T_PROD_STAG.pc_producercode pp on pup.ProducerCodeID_stg=pp.ID_stg

left join DB_T_PROD_STAG.pc_producercoderole ppr on pp.id_stg = ppr.ProducerCodeID_stg  /*EIM -35544*/

left join DB_T_PROD_STAG.pc_role pr on pr.id_stg = ppr.roleid_stg  /*EIM -35544*/

where 

pcg.typecode_stg = ''servicecenter_alfa''

and pp.code_stg is not null

and pg.namedenorm_stg is not null

and pr.name_stg = ''Agent'' /*EIM - 35544*/

) Temp

WHERE Temp.RN = 1



/***********************************Sales Hierarchy END***********************************/



UNION



/*********************************** Underwriter Hierarchy Start***********************************/

/************UW Relationship between ALFA and UW Home Office*****/

SELECT DISTINCT

''PRTY_RLTD_ROLE33'' as Relationship, /*  Need to add value in XLAT table */
pcg.TYPECODE_stg,

pg.name_stg as PNI_AddressBookUID,

childgrouptype.typecode_stg,

child.name_stg AS OTHER_AddressBookUID,

CAST(''1990-01-01 00:00:00.000000'' AS timestamp(6)) AS eff_dt, /*  Need to follow up with Ahmed			 */
CAST(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS end_dt, /*  Need to follow up with Ahmed		 */
cast(current_timestamp as timestamp(6))  AS UpdateTime,

''INTRNL_ORG'' as Lookups, 

 case when pg.retired_stg=0 and child.retired_stg=0 then 0 else 1 end as Retired,

''PRTY_STRC_TYPE13'' AS Structure



from DB_T_PROD_STAG.pc_parentgroup ppg INNER JOIN DB_T_PROD_STAG.pc_group pg on pg.id_stg=ppg.ForeignEntityID_stg

INNER JOIN DB_T_PROD_STAG.pc_group child on child.id_stg=ppg.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pctl_grouptype pcg on pcg.id_stg=pg.GroupType_stg

  INNER JOIN DB_T_PROD_STAG.pctl_grouptype childgrouptype on childgrouptype.id_stg=child.GroupType_stg

where pcg.TYPECODE_stg in (''root'')

and childgrouptype.TYPECODE_stg in (''homeofficeuw'')



UNION



/**UW Office to State***/

SELECT DISTINCT

''PRTY_RLTD_ROLE34'' as Relationship, /*  Need to add value in XLAT table */
childgrouptype.typecode_stg,

child.name_stg AS OTHER_AddressBookUID,

''INTRNL_ORG_SBTYPE5'' AS Name,

prz.Code_stg,

CAST(''1990-01-01 00:00:00.000000'' AS timestamp(6)) AS eff_dt, /*  Need to follow up with Ahmed			 */
CAST(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS end_dt, /*  Need to follow up with Ahmed		 */
cast(current_timestamp as timestamp(6))  AS UpdateTime,

''INTRNL_ORG'' as Lookups, 

 case when pg.retired_stg=0 and child.retired_stg=0 and district.retired_stg=0 and pr.retired_stg=0 then 0 else 1 end as Retired,

''PRTY_STRC_TYPE13'' AS Structure

FROM

/** ALFA to UW Home Office **/

DB_T_PROD_STAG.pc_parentgroup ppg INNER JOIN DB_T_PROD_STAG.pc_group pg on pg.id_stg=ppg.ForeignEntityID_stg

INNER JOIN DB_T_PROD_STAG.pc_group child on child.id_stg=ppg.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pctl_grouptype pcg on pcg.id_stg=pg.GroupType_stg

  INNER JOIN DB_T_PROD_STAG.pctl_grouptype childgrouptype on childgrouptype.id_stg=child.GroupType_stg

/** UW Office to UW District **/  

INNER JOIN DB_T_PROD_STAG.pc_parentgroup ho_to_district on ho_to_district.ForeignEntityID_stg=ppg.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pc_group district on district.id_stg=ho_to_district.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pctl_grouptype districttype on districttype.id_stg=district.GroupType_stg

/** Distrit to Region***/

INNER JOIN DB_T_PROD_STAG.pc_groupregion pgr on pgr.GroupID_stg=ho_to_district.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pc_region pr on pr.id_stg=pgr.RegionID_stg

/**Region to State**/

INNER JOIN DB_T_PROD_STAG.pc_region_zone prz on prz.RegionId_stg=pgr.RegionID_stg

where pcg.TYPECODE_stg in (''root'')

and childgrouptype.TYPECODE_stg in (''homeofficeuw'')

UNION

/**UW State to District****/

SELECT DISTINCT

''PRTY_RLTD_ROLE35'' as Relationship, /*  Need to add value in XLAT table */
''INTRNL_ORG_SBTYPE5'' AS Name,

prz.Code_stg,

districttype.TYPECODE_stg,

district.name_stg,

CAST(''1990-01-01 00:00:00.000000'' AS timestamp(6)) AS eff_dt, /*  Need to follow up with Ahmed			 */
CAST(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS end_dt, /*  Need to follow up with Ahmed		 */
cast(current_timestamp as timestamp(6))  AS UpdateTime,	

''INTRNL_ORG'' as Lookups, 

 case when pg.retired_stg=0 and child.retired_stg=0 and district.retired_stg=0 and pr.retired_stg=0 then 0 else 1 end as Retired,

''PRTY_STRC_TYPE13'' AS Structure

FROM

/** ALFA to UW Home Office **/

DB_T_PROD_STAG.pc_parentgroup ppg INNER JOIN DB_T_PROD_STAG.pc_group pg on pg.id_stg=ppg.ForeignEntityID_stg

INNER JOIN DB_T_PROD_STAG.pc_group child on child.id_stg=ppg.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pctl_grouptype pcg on pcg.id_stg=pg.GroupType_stg

  INNER JOIN DB_T_PROD_STAG.pctl_grouptype childgrouptype on childgrouptype.id_stg=child.GroupType_stg

/** UW Office to UW District **/  

INNER JOIN DB_T_PROD_STAG.pc_parentgroup ho_to_district on ho_to_district.ForeignEntityID_stg=ppg.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pc_group district on district.id_stg=ho_to_district.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pctl_grouptype districttype on districttype.id_stg=district.GroupType_stg

/** Distrit to Region***/

INNER JOIN DB_T_PROD_STAG.pc_groupregion pgr on pgr.GroupID_stg=ho_to_district.OwnerID_stg

INNER JOIN DB_T_PROD_STAG.pc_region pr on pr.id_stg=pgr.RegionID_stg

/**Region to State**/

INNER JOIN DB_T_PROD_STAG.pc_region_zone prz on prz.RegionId_stg=pgr.RegionID_stg

where pcg.TYPECODE_stg in (''root'',''homeofficeuw'')

and childgrouptype.TYPECODE_stg in (''homeofficeuw'')

UNION

/***************UW District to Underwriter Relationship************/

select DISTINCT

''PRTY_RLTD_ROLE36'' as Relationship, /*  Need to add value in XLAT table */
  pcg.TYPECODE_stg,

pg.Name_stg as PNI_AddressBookUID, /* UW District */
pc.PublicID_stg AS OTHER_AddressBookUID, /* Underwriter */
  cast(null as varchar(30)) AS Name1,

CAST(''1990-01-01 00:00:00.000000'' AS timestamp(6)) AS eff_dt, /*  Need to follow up with Ahmed			 */
CAST(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS end_dt, /*  Need to follow up with Ahmed		 */
cast(current_timestamp as timestamp(6))  AS UpdateTime,	

''INTRNL_ORG Claim_INDIV'' as Lookups, 

 case when pu.retired_stg=0 and pg.retired_stg=0 and pc.retired_stg=0 then 0 else 1 end as Retired,

''PRTY_STRC_TYPE14'' AS Structure

from DB_T_PROD_STAG.pc_groupuser pgu INNER JOIN DB_T_PROD_STAG.pc_group pg on pg.id_stg=pgu.GroupID_stg

INNER JOIN DB_T_PROD_STAG.pc_user pu on pu.id_stg=pgu.UserID_stg

INNER JOIN DB_T_PROD_STAG.pc_contact pc on pc.id_stg=pu.ContactID_stg

INNER JOIN DB_T_PROD_STAG.pctl_grouptype pcg on pcg.id_stg=pg.GroupType_stg

where pcg.TYPECODE_stg=''underwritingdistrict_alfa''

) as a where a.Relationship IN (''PRTY_RLTD_ROLE3'',''PRTY_RLTD_ROLE27'',''PRTY_RLTD_ROLE28'',''PRTY_RLTD_ROLE30'')

QUALIFY ROW_NUMBER() OVER (PARTITION BY PNI_AddressBookUID,OTHER_AddressBookUID,Relationship,Code,Name1,Lookups ORDER BY updatetime DESC)=1

 order by effectivedate asc,updatetime asc
) SRC
)
);


-- Component exp_all_source, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_all_source AS
(
SELECT
SQ_pc_prty_rltd_x_PRINS.Relationship as TYPECODE,
DECODE ( TRUE , LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT */ IS NULL , SQ_pc_prty_rltd_x_PRINS.Relationship , LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT */ ) as var_TYPECODE,
DECODE ( TRUE , var_TYPECODE IS NULL , ''UNK'' , var_TYPECODE ) as var1_TYPECODE,
LKP_3.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_INTRNL_ORG_SBTYPE */ as var_Code,
LKP_4.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_INTRNL_ORG_TYPE */ as INTRNL_ORG_TYPE_CD,
LKP_5.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_INTRNL_ORG_SBTYPE */ as INTRNL_ORG_SBTYPE_CD,
LKP_6.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_INTRNL_ORG_SBTYPE */ as var_Name,
LKP_7.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SRC_CD */ as v_SYS_SRC_CD,
DECODE ( TRUE , UPPER ( SQ_pc_prty_rltd_x_PRINS.Lookups ) = ''BUSN POLICY_INDIV'' , DECODE ( TRUE , LKP_8.INDIV_PRTY_ID /* replaced lookup LKP_INDIV_CNT_MGR */ IS NOT NULL , LKP_9.INDIV_PRTY_ID /* replaced lookup LKP_INDIV_CNT_MGR */ , LKP_10.BUSN_PRTY_ID /* replaced lookup LKP_BUSN */ ) , UPPER ( SQ_pc_prty_rltd_x_PRINS.Lookups ) = ''POLICY_INDIV'' , LKP_11.INDIV_PRTY_ID /* replaced lookup LKP_INDIV_CNT_MGR */ , UPPER ( SQ_pc_prty_rltd_x_PRINS.Lookups ) = ''CLAIM_INDIV INTRNL_ORG'' , LKP_12.INDIV_PRTY_ID /* replaced lookup LKP_INDIV_CLM_CTR */ , UPPER ( SQ_pc_prty_rltd_x_PRINS.Lookups ) = ''INTRNL_ORG'' , LKP_13.INTRNL_ORG_PRTY_ID /* replaced lookup LKP_INTRNL_ORG */ , UPPER ( SQ_pc_prty_rltd_x_PRINS.Lookups ) = ''INTRNL_ORG POLICY_INDIV'' , LKP_14.INTRNL_ORG_PRTY_ID /* replaced lookup LKP_INTRNL_ORG */ , UPPER ( SQ_pc_prty_rltd_x_PRINS.Lookups ) = ''INTRNL_ORG CLAIM_INDIV'' , LKP_15.INTRNL_ORG_PRTY_ID /* replaced lookup LKP_INTRNL_ORG */ ) as var_PRTY_ID,
CASE WHEN var_PRTY_ID IS NULL THEN 9999 ELSE var_PRTY_ID END as out_PRTY_ID,
DECODE ( TRUE , UPPER ( SQ_pc_prty_rltd_x_PRINS.Lookups ) = ''BUSN POLICY_INDIV'' , LKP_16.BUSN_PRTY_ID /* replaced lookup LKP_BUSN */ , UPPER ( SQ_pc_prty_rltd_x_PRINS.Lookups ) = ''POLICY_INDIV'' , LKP_17.INDIV_PRTY_ID /* replaced lookup LKP_INDIV_CNT_MGR */ , UPPER ( SQ_pc_prty_rltd_x_PRINS.Lookups ) = ''INTRNL_ORG'' , LKP_18.INTRNL_ORG_PRTY_ID /* replaced lookup LKP_INTRNL_ORG */ , UPPER ( SQ_pc_prty_rltd_x_PRINS.Lookups ) = ''CLAIM_INDIV INTRNL_ORG'' , LKP_19.INTRNL_ORG_PRTY_ID /* replaced lookup LKP_INTRNL_ORG */ , UPPER ( SQ_pc_prty_rltd_x_PRINS.Lookups ) = ''INTRNL_ORG POLICY_INDIV'' , LKP_20.INDIV_PRTY_ID /* replaced lookup LKP_INDIV_CLM_CTR */ , UPPER ( SQ_pc_prty_rltd_x_PRINS.Lookups ) = ''INTRNL_ORG CLAIM_INDIV'' , LKP_21.INDIV_PRTY_ID /* replaced lookup LKP_INDIV_CLM_CTR */ ) as var_RLTD_PRTY_ID,
CASE WHEN var_RLTD_PRTY_ID IS NULL THEN 9999 ELSE var_RLTD_PRTY_ID END as out_RLTD_PRTY_ID,
SQ_pc_prty_rltd_x_PRINS.EffectiveDate as in_PRTY_RLTD_STRT_DTTM,
SQ_pc_prty_rltd_x_PRINS.ExpirationDate as PRTY_RLTD_END_DTTM,
DECODE ( TRUE , LKP_22.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_PRTY_STRC */ IS NULL , ''UNK'' , LKP_23.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_PRTY_STRC */ ) as PRTY_STRC_TYPE_CD,
:PRCS_ID as PRCS_ID,
SQ_pc_prty_rltd_x_PRINS.updatetime as in_Trans_strt_dttm,
SQ_pc_prty_rltd_x_PRINS.EffectiveDate as EffectiveDate,
SQ_pc_prty_rltd_x_PRINS.Retired as Retired,
SQ_pc_prty_rltd_x_PRINS.source_record_id,
row_number() over (partition by SQ_pc_prty_rltd_x_PRINS.source_record_id order by SQ_pc_prty_rltd_x_PRINS.source_record_id) as RNK
FROM
SQ_pc_prty_rltd_x_PRINS
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = SQ_pc_prty_rltd_x_PRINS.Relationship
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = SQ_pc_prty_rltd_x_PRINS.Relationship
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_INTRNL_ORG_SBTYPE LKP_3 ON LKP_3.SRC_IDNTFTN_VAL = SQ_pc_prty_rltd_x_PRINS.Code
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_INTRNL_ORG_TYPE LKP_4 ON LKP_4.SRC_IDNTFTN_VAL = ''INTRNL_ORG_TYPE15''
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_INTRNL_ORG_SBTYPE LKP_5 ON LKP_5.SRC_IDNTFTN_VAL = SQ_pc_prty_rltd_x_PRINS.PNI_AddressBookUID
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_INTRNL_ORG_SBTYPE LKP_6 ON LKP_6.SRC_IDNTFTN_VAL = SQ_pc_prty_rltd_x_PRINS.Name
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_SRC_CD LKP_7 ON LKP_7.SRC_IDNTFTN_VAL = SQ_pc_prty_rltd_x_PRINS.SYS_SRC_CD
LEFT JOIN LKP_INDIV_CNT_MGR LKP_8 ON LKP_8.NK_LINK_ID = SQ_pc_prty_rltd_x_PRINS.PNI_AddressBookUID
LEFT JOIN LKP_INDIV_CNT_MGR LKP_9 ON LKP_9.NK_LINK_ID = SQ_pc_prty_rltd_x_PRINS.PNI_AddressBookUID
LEFT JOIN LKP_BUSN LKP_10 ON LKP_10.BUSN_CTGY_CD = ''CO'' AND LKP_10.NK_BUSN_CD = SQ_pc_prty_rltd_x_PRINS.PNI_AddressBookUID
LEFT JOIN LKP_INDIV_CNT_MGR LKP_11 ON LKP_11.NK_LINK_ID = SQ_pc_prty_rltd_x_PRINS.PNI_AddressBookUID
LEFT JOIN LKP_INDIV_CLM_CTR LKP_12 ON LKP_12.NK_PUBLC_ID = SQ_pc_prty_rltd_x_PRINS.PNI_AddressBookUID
LEFT JOIN LKP_INTRNL_ORG LKP_13 ON LKP_13.INTRNL_ORG_TYPE_CD = INTRNL_ORG_TYPE_CD AND LKP_13.INTRNL_ORG_SBTYPE_CD = INTRNL_ORG_SBTYPE_CD AND LKP_13.INTRNL_ORG_NUM = SQ_pc_prty_rltd_x_PRINS.OTHER_AddressBookUID
LEFT JOIN LKP_INTRNL_ORG LKP_14 ON LKP_14.INTRNL_ORG_TYPE_CD = INTRNL_ORG_TYPE_CD AND LKP_14.INTRNL_ORG_SBTYPE_CD = INTRNL_ORG_SBTYPE_CD AND LKP_14.INTRNL_ORG_NUM = SQ_pc_prty_rltd_x_PRINS.OTHER_AddressBookUID
LEFT JOIN LKP_INTRNL_ORG LKP_15 ON LKP_15.INTRNL_ORG_TYPE_CD = INTRNL_ORG_TYPE_CD AND LKP_15.INTRNL_ORG_SBTYPE_CD = INTRNL_ORG_SBTYPE_CD AND LKP_15.INTRNL_ORG_NUM = SQ_pc_prty_rltd_x_PRINS.OTHER_AddressBookUID
LEFT JOIN LKP_BUSN LKP_16 ON LKP_16.BUSN_CTGY_CD = ''INSCAR'' AND LKP_16.NK_BUSN_CD = SQ_pc_prty_rltd_x_PRINS.OTHER_AddressBookUID
LEFT JOIN LKP_INDIV_CNT_MGR LKP_17 ON LKP_17.NK_LINK_ID = SQ_pc_prty_rltd_x_PRINS.OTHER_AddressBookUID
LEFT JOIN LKP_INTRNL_ORG LKP_18 ON LKP_18.INTRNL_ORG_TYPE_CD = ''INT'' AND LKP_18.INTRNL_ORG_SBTYPE_CD = UPPER ( var_Code ) AND LKP_18.INTRNL_ORG_NUM = UPPER ( SQ_pc_prty_rltd_x_PRINS.Name )
LEFT JOIN LKP_INTRNL_ORG LKP_19 ON LKP_19.INTRNL_ORG_TYPE_CD = ''INT'' AND LKP_19.INTRNL_ORG_SBTYPE_CD = UPPER ( var_Name ) AND LKP_19.INTRNL_ORG_NUM = UPPER ( SQ_pc_prty_rltd_x_PRINS.OTHER_AddressBookUID )
LEFT JOIN LKP_INDIV_CLM_CTR LKP_20 ON LKP_20.NK_PUBLC_ID = SQ_pc_prty_rltd_x_PRINS.Code
LEFT JOIN LKP_INDIV_CLM_CTR LKP_21 ON LKP_21.NK_PUBLC_ID = SQ_pc_prty_rltd_x_PRINS.Code
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_PRTY_STRC LKP_22 ON LKP_22.SRC_IDNTFTN_VAL = SQ_pc_prty_rltd_x_PRINS.Party_Structure_Type_Cd
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_PRTY_STRC LKP_23 ON LKP_23.SRC_IDNTFTN_VAL = SQ_pc_prty_rltd_x_PRINS.Party_Structure_Type_Cd
QUALIFY RNK = 1
);


-- Component LKP_TERADATA_ETL_REF_XLAT_RELATIONSHIP, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_RELATIONSHIP AS
(
SELECT
LKP.TGT_IDNTFTN_VAL,
exp_all_source.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_all_source.source_record_id ORDER BY LKP.TGT_IDNTFTN_VAL desc,LKP.SRC_IDNTFTN_VAL desc) RNK
FROM
exp_all_source
LEFT JOIN (
SELECT 
	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL
	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 
FROM 
	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT
WHERE 
	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM in ( ''PRTY_RLTD_ROLE'')
             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM in  (''derived'',''pc_role.name'') 
		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS in (''DS'',''GW'') 
		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
) LKP ON LKP.SRC_IDNTFTN_VAL = exp_all_source.TYPECODE
QUALIFY RNK = 1
);


-- Component Rnk, Type RANK 
CREATE OR REPLACE TEMPORARY TABLE Rnk AS
(
SELECT * FROM (
SELECT
RANKINDEX as RANKINDEX,
exp_all_source.out_PRTY_ID as out_PRTY_ID,
exp_all_source.out_RLTD_PRTY_ID as out_RLTD_PRTY_ID,
LKP_TERADATA_ETL_REF_XLAT_RELATIONSHIP.TGT_IDNTFTN_VAL as in_PRTY_RLTD_ROLE_CD,
exp_all_source.in_PRTY_RLTD_STRT_DTTM as out_PRTY_RLTD_STRT_DTTM,
exp_all_source.PRTY_RLTD_END_DTTM as PRTY_RLTD_END_DTTM,
exp_all_source.PRTY_STRC_TYPE_CD as PRTY_STRC_TYPE_CD,
exp_all_source.PRCS_ID as PRCS_ID,
exp_all_source.Retired as Retired,
exp_all_source.in_Trans_strt_dttm as out_Trans_strt_dttm,
exp_all_source.source_record_id
FROM
exp_all_source
INNER JOIN LKP_TERADATA_ETL_REF_XLAT_RELATIONSHIP ON exp_all_source.source_record_id = LKP_TERADATA_ETL_REF_XLAT_RELATIONSHIP.source_record_id
)
WHERE out_Trans_strt_dttm <= 100
);


-- Component exp_SrcFields, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_SrcFields AS
(
SELECT
Rnk.out_PRTY_ID as in_PRTY_ID,
Rnk.out_RLTD_PRTY_ID as in_RLTD_PRTY_ID,
Rnk.in_PRTY_RLTD_ROLE_CD as in_PRTY_RLTD_ROLE_CD,
Rnk.out_PRTY_RLTD_STRT_DTTM as in_PRTY_RLTD_STRT_DTTM,
CASE WHEN Rnk.out_PRTY_RLTD_STRT_DTTM IS NULL THEN TO_TIMESTAMP(''01/01/1900'', ''MM/DD/YYYY'') ELSE Rnk.out_PRTY_RLTD_STRT_DTTM END as in_PRTY_RLTD_STRT_DTTM1,
CASE WHEN Rnk.PRTY_RLTD_END_DTTM IS NULL THEN TO_TIMESTAMP(''12/31/9999 23:59:59.999999'', ''MM/DD/YYYY HH24:MI:SS.NS'') ELSE Rnk.PRTY_RLTD_END_DTTM END as o_PRTY_RLTD_END_DTTM,
Rnk.PRTY_STRC_TYPE_CD as in_PRTY_STRC_TYPE_CD,
Rnk.PRCS_ID as in_PRCS_ID,
Rnk.out_Trans_strt_dttm as Trans_strt_dttm,
Rnk.Retired as Retired,
CURRENT_TIMESTAMP as EDW_STRT_DTTM,
TO_TIMESTAMP(''12/31/9999 23:59:59.999999'', ''MM/DD/YYYY HH24:MI:SS.NS'') as EDW_END_DTTM,
DATEADD(''ss'', - 1, CURRENT_TIMESTAMP) as EDW_END_DTTM_exp,
Rnk.RANKINDEX as RANKINDEX,
Rnk.source_record_id
FROM
Rnk
);


-- Component LKP_PRTY_RLTD_CDC, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_PRTY_RLTD_CDC AS
(
SELECT
LKP.PRTY_ID,
LKP.RLTD_PRTY_ID,
LKP.PRTY_RLTD_ROLE_CD,
LKP.PRTY_RLTD_STRT_DTTM,
LKP.PRTY_RLTD_END_DTTM,
LKP.PRTY_STRC_TYPE_CD,
LKP.EDW_STRT_DTTM,
LKP.EDW_END_DTTM,
LKP.TRANS_STRT_DTTM,
exp_SrcFields.in_PRTY_ID as in_PRTY_ID,
exp_SrcFields.in_RLTD_PRTY_ID as in_RLTD_PRTY_ID,
exp_SrcFields.in_PRTY_RLTD_ROLE_CD as in_PRTY_RLTD_ROLE_CD,
exp_SrcFields.in_PRTY_RLTD_STRT_DTTM1 as in_PRTY_RLTD_STRT_DTTM,
exp_SrcFields.o_PRTY_RLTD_END_DTTM as o_PRTY_RLTD_END_DTTM,
exp_SrcFields.in_PRTY_STRC_TYPE_CD as in_PRTY_STRC_TYPE_CD,
exp_SrcFields.in_PRCS_ID as in_PRCS_ID,
exp_SrcFields.Trans_strt_dttm as Trans_strt_dttm1,
exp_SrcFields.EDW_STRT_DTTM as EDW_STRT_DTTM1,
exp_SrcFields.EDW_END_DTTM as EDW_END_DTTM1,
exp_SrcFields.EDW_END_DTTM_exp as EDW_END_DTTM_exp,
exp_SrcFields.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_SrcFields.source_record_id ORDER BY LKP.PRTY_ID asc,LKP.RLTD_PRTY_ID asc,LKP.PRTY_RLTD_ROLE_CD asc,LKP.PRTY_RLTD_STRT_DTTM asc,LKP.PRTY_RLTD_END_DTTM asc,LKP.PRTY_STRC_TYPE_CD asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc,LKP.TRANS_STRT_DTTM asc) RNK
FROM
exp_SrcFields
LEFT JOIN (
SELECT PRTY_RLTD.PRTY_RLTD_STRT_DTTM as PRTY_RLTD_STRT_DTTM, PRTY_RLTD.PRTY_RLTD_END_DTTM as PRTY_RLTD_END_DTTM, PRTY_RLTD.TRANS_STRT_DTTM as TRANS_STRT_DTTM,PRTY_RLTD.PRTY_STRC_TYPE_CD as PRTY_STRC_TYPE_CD,  PRTY_RLTD.EDW_STRT_DTTM as EDW_STRT_DTTM, PRTY_RLTD.EDW_END_DTTM as EDW_END_DTTM, PRTY_RLTD.PRTY_ID as PRTY_ID, PRTY_RLTD.RLTD_PRTY_ID as RLTD_PRTY_ID, PRTY_RLTD.PRTY_RLTD_ROLE_CD as PRTY_RLTD_ROLE_CD FROM DB_T_PROD_CORE.PRTY_RLTD WHERE PRTY_RLTD_ROLE_CD in (''PRIINSCAR'',''DISTTOMGR'',''DISTTOSPRVSR'',''PRDASVC'')
QUALIFY ROW_NUMBER() OVER(PARTITION BY PRTY_ID,PRTY_RLTD_ROLE_CD ORDER BY EDW_END_DTTM desc) = 1
) LKP ON LKP.PRTY_ID = exp_SrcFields.in_PRTY_ID AND LKP.PRTY_RLTD_ROLE_CD = exp_SrcFields.in_PRTY_RLTD_ROLE_CD
QUALIFY RNK = 1
);


-- Component exp_CDC_Check, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_CDC_Check AS
(
SELECT
LKP_PRTY_RLTD_CDC.in_PRTY_ID as in_PRTY_ID,
LKP_PRTY_RLTD_CDC.in_RLTD_PRTY_ID as in_RLTD_PRTY_ID,
LKP_PRTY_RLTD_CDC.in_PRTY_RLTD_ROLE_CD as in_PRTY_RLTD_ROLE_CD,
LKP_PRTY_RLTD_CDC.in_PRTY_RLTD_STRT_DTTM as in_PRTY_RLTD_STRT_DTTM,
LKP_PRTY_RLTD_CDC.o_PRTY_RLTD_END_DTTM as in_PRTY_RLTD_END_DTTM,
LKP_PRTY_RLTD_CDC.in_PRTY_STRC_TYPE_CD as in_PRTY_STRC_TYPE_CD,
exp_SrcFields.in_PRCS_ID as in_PRCS_ID,
LKP_PRTY_RLTD_CDC.PRTY_ID as lkp_PRTY_ID,
LKP_PRTY_RLTD_CDC.RLTD_PRTY_ID as lkp_RLTD_PRTY_ID,
LKP_PRTY_RLTD_CDC.PRTY_RLTD_ROLE_CD as lkp_PRTY_RLTD_ROLE_CD,
LKP_PRTY_RLTD_CDC.PRTY_RLTD_STRT_DTTM as lkp_PRTY_RLTD_STRT_DTTM,
LKP_PRTY_RLTD_CDC.PRTY_RLTD_END_DTTM as lkp_PRTY_RLTD_END_DTTM,
LKP_PRTY_RLTD_CDC.PRTY_STRC_TYPE_CD as lkp_PRTY_STRC_TYPE_CD,
NULL as lkp_PRCS_ID,
LKP_PRTY_RLTD_CDC.EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
LKP_PRTY_RLTD_CDC.EDW_END_DTTM as lkp_EDW_END_DTTM,
md5 ( ltrim ( rtrim ( to_char ( LKP_PRTY_RLTD_CDC.in_PRTY_RLTD_STRT_DTTM ) ) ) || ltrim ( rtrim ( to_char ( LKP_PRTY_RLTD_CDC.o_PRTY_RLTD_END_DTTM ) ) ) || ltrim ( rtrim ( LKP_PRTY_RLTD_CDC.in_PRTY_STRC_TYPE_CD ) ) || TO_CHAR ( LKP_PRTY_RLTD_CDC.in_RLTD_PRTY_ID ) ) as var_calc_chksm,
md5 ( ltrim ( rtrim ( to_char ( LKP_PRTY_RLTD_CDC.PRTY_RLTD_STRT_DTTM ) ) ) || ltrim ( rtrim ( to_char ( LKP_PRTY_RLTD_CDC.PRTY_RLTD_END_DTTM ) ) ) || ltrim ( rtrim ( LKP_PRTY_RLTD_CDC.PRTY_STRC_TYPE_CD ) ) || TO_CHAR ( LKP_PRTY_RLTD_CDC.RLTD_PRTY_ID ) ) as var_orig_chksm,
CASE WHEN var_orig_chksm IS NULL THEN ''I'' ELSE CASE WHEN var_orig_chksm != var_calc_chksm THEN ''U'' ELSE ''R'' END END as o_flag_MD5,
LKP_PRTY_RLTD_CDC.EDW_STRT_DTTM1 as StartTime,
LKP_PRTY_RLTD_CDC.EDW_END_DTTM1 as EndTime,
LKP_PRTY_RLTD_CDC.Trans_strt_dttm1 as Trans_strt_dttm,
LKP_PRTY_RLTD_CDC.EDW_END_DTTM_exp as EDW_END_DTTM_exp,
exp_SrcFields.Retired as Retired,
exp_SrcFields.RANKINDEX as RANKINDEX,
exp_SrcFields.source_record_id
FROM
exp_SrcFields
INNER JOIN LKP_PRTY_RLTD_CDC ON exp_SrcFields.source_record_id = LKP_PRTY_RLTD_CDC.source_record_id
);


-- Component rtr_CDC_Insert, Type ROUTER Output Group Insert
CREATE OR REPLACE TEMPORARY TABLE rtr_CDC_Insert AS (
SELECT
exp_CDC_Check.in_PRTY_ID as in_PRTY_ID,
exp_CDC_Check.in_RLTD_PRTY_ID as in_RLTD_PRTY_ID,
exp_CDC_Check.in_PRTY_RLTD_ROLE_CD as in_PRTY_RLTD_ROLE_CD,
exp_CDC_Check.in_PRTY_RLTD_STRT_DTTM as in_PRTY_RLTD_STRT_DTTM,
exp_CDC_Check.in_PRTY_RLTD_END_DTTM as in_PRTY_RLTD_END_DTTM,
exp_CDC_Check.in_PRTY_STRC_TYPE_CD as in_PRTY_STRC_TYPE_CD,
exp_CDC_Check.in_PRCS_ID as in_PRCS_ID,
exp_CDC_Check.lkp_PRTY_ID as lkp_PRTY_ID,
exp_CDC_Check.lkp_RLTD_PRTY_ID as lkp_RLTD_PRTY_ID,
exp_CDC_Check.lkp_PRTY_RLTD_ROLE_CD as lkp_PRTY_RLTD_ROLE_CD,
exp_CDC_Check.lkp_PRTY_RLTD_STRT_DTTM as lkp_PRTY_RLTD_STRT_DTTM,
exp_CDC_Check.lkp_PRTY_RLTD_END_DTTM as lkp_PRTY_RLTD_END_DTTM,
exp_CDC_Check.lkp_PRTY_STRC_TYPE_CD as lkp_PRTY_STRC_TYPE_CD,
exp_CDC_Check.lkp_PRCS_ID as lkp_PRCS_ID,
exp_CDC_Check.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_CDC_Check.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
NULL as o_Src_Tgt,
exp_CDC_Check.StartTime as StartTime,
exp_CDC_Check.EndTime as EndTime,
exp_CDC_Check.Trans_strt_dttm as Trans_strt_dttm,
exp_CDC_Check.EDW_END_DTTM_exp as EDW_END_DTTM_exp,
exp_CDC_Check.Retired as Retired,
exp_CDC_Check.o_flag_MD5 as o_flag_MD5,
exp_CDC_Check.RANKINDEX as RANKINDEX,
exp_CDC_Check.source_record_id
FROM
exp_CDC_Check
WHERE CASE WHEN exp_CDC_Check.o_flag_MD5 = ''I'' AND exp_CDC_Check.in_PRTY_ID != 9999 AND exp_CDC_Check.in_RLTD_PRTY_ID != 9999 THEN 1 ELSE CASE WHEN exp_CDC_Check.o_flag_MD5 = ''U'' AND exp_CDC_Check.lkp_EDW_END_DTTM = TO_TIMESTAMP(''12/31/9999 23:59:59.999999'', ''MM/DD/YYYY HH24:MI:SS.NS'') THEN 1 ELSE CASE WHEN exp_CDC_Check.Retired = 0 AND exp_CDC_Check.lkp_EDW_END_DTTM != TO_TIMESTAMP(''12/31/9999 23:59:59.999999'', ''MM/DD/YYYY HH24:MI:SS.NS'') THEN 1 ELSE 0 END END END - - o_Src_Tgt = ''I'' OR ( exp_CDC_Check.Retired = 0 AND exp_CDC_Check.lkp_EDW_END_DTTM != TO_TIMESTAMP(''12/31/9999 23:59:59.999999'', ''MM/DD/YYYY HH24:MI:SS.NS'') and exp_CDC_Check.lkp_PRTY_ID IS NOT NULL and exp_CDC_Check.lkp_RLTD_PRTY_ID IS NOT NULL )
);


-- Component rtr_CDC_Retired, Type ROUTER Output Group Retired
CREATE OR REPLACE TEMPORARY TABLE rtr_CDC_Retired AS (
SELECT
exp_CDC_Check.in_PRTY_ID as in_PRTY_ID,
exp_CDC_Check.in_RLTD_PRTY_ID as in_RLTD_PRTY_ID,
exp_CDC_Check.in_PRTY_RLTD_ROLE_CD as in_PRTY_RLTD_ROLE_CD,
exp_CDC_Check.in_PRTY_RLTD_STRT_DTTM as in_PRTY_RLTD_STRT_DTTM,
exp_CDC_Check.in_PRTY_RLTD_END_DTTM as in_PRTY_RLTD_END_DTTM,
exp_CDC_Check.in_PRTY_STRC_TYPE_CD as in_PRTY_STRC_TYPE_CD,
exp_CDC_Check.in_PRCS_ID as in_PRCS_ID,
exp_CDC_Check.lkp_PRTY_ID as lkp_PRTY_ID,
exp_CDC_Check.lkp_RLTD_PRTY_ID as lkp_RLTD_PRTY_ID,
exp_CDC_Check.lkp_PRTY_RLTD_ROLE_CD as lkp_PRTY_RLTD_ROLE_CD,
exp_CDC_Check.lkp_PRTY_RLTD_STRT_DTTM as lkp_PRTY_RLTD_STRT_DTTM,
exp_CDC_Check.lkp_PRTY_RLTD_END_DTTM as lkp_PRTY_RLTD_END_DTTM,
exp_CDC_Check.lkp_PRTY_STRC_TYPE_CD as lkp_PRTY_STRC_TYPE_CD,
exp_CDC_Check.lkp_PRCS_ID as lkp_PRCS_ID,
exp_CDC_Check.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_CDC_Check.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
NULL as o_Src_Tgt,
exp_CDC_Check.StartTime as StartTime,
exp_CDC_Check.EndTime as EndTime,
exp_CDC_Check.Trans_strt_dttm as Trans_strt_dttm,
exp_CDC_Check.EDW_END_DTTM_exp as EDW_END_DTTM_exp,
exp_CDC_Check.Retired as Retired,
exp_CDC_Check.o_flag_MD5 as o_flag_MD5,
exp_CDC_Check.RANKINDEX as RANKINDEX,
exp_CDC_Check.source_record_id
FROM
exp_CDC_Check
WHERE exp_CDC_Check.o_flag_MD5 = ''R'' and exp_CDC_Check.Retired != 0 and exp_CDC_Check.lkp_EDW_END_DTTM = TO_TIMESTAMP(''12/31/9999 23:59:59.999999'', ''MM/DD/YYYY HH24:MI:SS.NS'') - - o_Src_Tgt = ''R'' and exp_CDC_Check.Retired != 0 and exp_CDC_Check.lkp_EDW_END_DTTM = TO_TIMESTAMP(''12/31/9999 23:59:59.999999'', ''MM/DD/YYYY HH24:MI:SS.NS'')
);


-- Component upd_Upd_retired, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_Upd_retired AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_CDC_Retired.lkp_PRTY_ID as lkp_PRTY_ID3,
rtr_CDC_Retired.lkp_RLTD_PRTY_ID as lkp_RLTD_PRTY_ID3,
rtr_CDC_Retired.lkp_PRTY_RLTD_ROLE_CD as lkp_PRTY_RLTD_ROLE_CD3,
rtr_CDC_Retired.lkp_PRTY_RLTD_STRT_DTTM as lkp_PRTY_RLTD_STRT_DTTM3,
rtr_CDC_Retired.lkp_PRTY_RLTD_END_DTTM as lkp_PRTY_RLTD_END_DTTM3,
NULL as lkp_PRTY_STRC_TYPE_CD3,
rtr_CDC_Retired.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM3,
NULL as EDW_END_DTTM_exp3,
rtr_CDC_Retired.Trans_strt_dttm as Trans_strt_dttm4,
rtr_CDC_Retired.source_record_id,
1 as UPDATE_STRATEGY_ACTION
FROM
rtr_CDC_Retired
);


-- Component upd_insert, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_insert AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_CDC_Insert.in_PRTY_ID as in_PRTY_ID1,
rtr_CDC_Insert.in_RLTD_PRTY_ID as in_RLTD_PRTY_ID1,
rtr_CDC_Insert.in_PRTY_RLTD_ROLE_CD as in_PRTY_RLTD_ROLE_CD1,
rtr_CDC_Insert.in_PRTY_RLTD_STRT_DTTM as in_PRTY_RLTD_STRT_DTTM1,
rtr_CDC_Insert.in_PRTY_RLTD_END_DTTM as in_PRTY_RLTD_END_DTTM1,
rtr_CDC_Insert.in_PRTY_STRC_TYPE_CD as in_PRTY_STRC_TYPE_CD1,
rtr_CDC_Insert.in_PRCS_ID as in_PRCS_ID1,
rtr_CDC_Insert.StartTime as StartTime1,
rtr_CDC_Insert.EndTime as EndTime1,
rtr_CDC_Insert.Trans_strt_dttm as Trans_strt_dttm1,
rtr_CDC_Insert.Retired as Retired1,
rtr_CDC_Insert.RANKINDEX as RANKINDEX1,
rtr_CDC_Insert.source_record_id,
0 as UPDATE_STRATEGY_ACTION
FROM
rtr_CDC_Insert
);


-- Component exp_prty_rltd_insert, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_prty_rltd_insert AS
(
SELECT
upd_insert.in_PRTY_ID1 as in_PRTY_ID1,
upd_insert.in_RLTD_PRTY_ID1 as in_RLTD_PRTY_ID1,
upd_insert.in_PRTY_RLTD_ROLE_CD1 as in_PRTY_RLTD_ROLE_CD1,
upd_insert.in_PRTY_RLTD_STRT_DTTM1 as in_PRTY_RLTD_STRT_DTTM1,
upd_insert.in_PRTY_RLTD_END_DTTM1 as in_PRTY_RLTD_END_DTTM1,
upd_insert.in_PRTY_STRC_TYPE_CD1 as in_PRTY_STRC_TYPE_CD1,
upd_insert.in_PRCS_ID1 as in_PRCS_ID1,
upd_insert.Trans_strt_dttm1 as Trans_strt_dttm1,
CASE WHEN upd_insert.Retired1 = 0 THEN TO_TIMESTAMP(''9999-12-31 23:59:59.999999'', ''YYYY-MM-DD HH24:MI:SS.NS'') ELSE CURRENT_TIMESTAMP END as out_EDW_END_DTTM,
CASE WHEN upd_insert.Retired1 != 0 THEN upd_insert.Trans_strt_dttm1 ELSE TO_TIMESTAMP(''9999-12-31 23:59:59.999999'', ''YYYY-MM-DD HH24:MI:SS.NS'') END as TRANS_END_DTTM,
DATEADD(''ss'', ( 2 * ( upd_insert.RANKINDEX1 - 1 ) ), CURRENT_TIMESTAMP) as EDW_STRT_DTTM,
upd_insert.source_record_id
FROM
upd_insert
);


-- Component EXPTRANS_upd_retired, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXPTRANS_upd_retired AS
(
SELECT
upd_Upd_retired.lkp_PRTY_ID3 as lkp_PRTY_ID3,
upd_Upd_retired.lkp_RLTD_PRTY_ID3 as lkp_RLTD_PRTY_ID3,
upd_Upd_retired.lkp_PRTY_RLTD_ROLE_CD3 as lkp_PRTY_RLTD_ROLE_CD3,
upd_Upd_retired.lkp_EDW_STRT_DTTM3 as lkp_EDW_STRT_DTTM3,
CURRENT_TIMESTAMP as out_EDW_END_DTTM,
upd_Upd_retired.Trans_strt_dttm4 as Trans_strt_dttm4,
upd_Upd_retired.source_record_id
FROM
upd_Upd_retired
);


-- Component tgt_prty_rltd_NewInsert_PRINS, Type TARGET 
INSERT INTO DB_T_PROD_CORE.PRTY_RLTD
(
PRTY_ID,
RLTD_PRTY_ID,
PRTY_RLTD_ROLE_CD,
PRTY_RLTD_STRT_DTTM,
PRTY_RLTD_END_DTTM,
PRTY_STRC_TYPE_CD,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM,
TRANS_END_DTTM
)
SELECT
exp_prty_rltd_insert.in_PRTY_ID1 as PRTY_ID,
exp_prty_rltd_insert.in_RLTD_PRTY_ID1 as RLTD_PRTY_ID,
exp_prty_rltd_insert.in_PRTY_RLTD_ROLE_CD1 as PRTY_RLTD_ROLE_CD,
exp_prty_rltd_insert.in_PRTY_RLTD_STRT_DTTM1 as PRTY_RLTD_STRT_DTTM,
exp_prty_rltd_insert.in_PRTY_RLTD_END_DTTM1 as PRTY_RLTD_END_DTTM,
exp_prty_rltd_insert.in_PRTY_STRC_TYPE_CD1 as PRTY_STRC_TYPE_CD,
exp_prty_rltd_insert.in_PRCS_ID1 as PRCS_ID,
exp_prty_rltd_insert.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_prty_rltd_insert.out_EDW_END_DTTM as EDW_END_DTTM,
exp_prty_rltd_insert.Trans_strt_dttm1 as TRANS_STRT_DTTM,
exp_prty_rltd_insert.TRANS_END_DTTM as TRANS_END_DTTM
FROM
exp_prty_rltd_insert;


-- Component tgt_prty_rltd_Upd_retired_PRINS, Type TARGET 
MERGE INTO DB_T_PROD_CORE.PRTY_RLTD
USING EXPTRANS_upd_retired ON (PRTY_RLTD.PRTY_ID = EXPTRANS_upd_retired.lkp_PRTY_ID3 AND PRTY_RLTD.RLTD_PRTY_ID = EXPTRANS_upd_retired.lkp_RLTD_PRTY_ID3 AND PRTY_RLTD.PRTY_RLTD_ROLE_CD = EXPTRANS_upd_retired.lkp_PRTY_RLTD_ROLE_CD3 AND PRTY_RLTD.EDW_STRT_DTTM = EXPTRANS_upd_retired.lkp_EDW_STRT_DTTM3)
WHEN MATCHED THEN UPDATE
SET
PRTY_ID = EXPTRANS_upd_retired.lkp_PRTY_ID3,
RLTD_PRTY_ID = EXPTRANS_upd_retired.lkp_RLTD_PRTY_ID3,
PRTY_RLTD_ROLE_CD = EXPTRANS_upd_retired.lkp_PRTY_RLTD_ROLE_CD3,
EDW_STRT_DTTM = EXPTRANS_upd_retired.lkp_EDW_STRT_DTTM3,
EDW_END_DTTM = EXPTRANS_upd_retired.out_EDW_END_DTTM,
TRANS_END_DTTM = EXPTRANS_upd_retired.Trans_strt_dttm4;


-- PIPELINE END FOR 3
-- Component tgt_prty_rltd_NewInsert_PRINS, Type Post SQL 
UPDATE  DB_T_PROD_CORE.PRTY_RLTD  
set TRANS_END_DTTM=  A.lead, EDW_END_DTTM=A.EDW_lead
FROM  

(

SELECT	distinct PRTY_ID,RLTD_PRTY_ID,EDW_STRT_DTTM,PRTY_RLTD_ROLE_CD,PRTY_RLTD_STRT_DTTM,

max(TRANS_STRT_DTTM) over (partition by PRTY_ID,PRTY_RLTD_ROLE_CD  ORDER BY EDW_STRT_DTTM  ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND'' 

 as lead ,max(EDW_STRT_DTTM) over (partition by PRTY_ID,PRTY_RLTD_ROLE_CD  ORDER BY EDW_STRT_DTTM  ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND'' 

 as EDW_lead

FROM DB_T_PROD_CORE.PRTY_RLTD   where  PRTY_RLTD_ROLE_CD in (''PRIINSCAR'',''DISTTOMGR'',''DISTTOSPRVSR'',''PRDASVC'') and PRTY_ID <> ''9999''


)  A



where  PRTY_RLTD.RLTD_PRTY_ID = A.RLTD_PRTY_ID

and PRTY_RLTD.PRTY_ID=A.PRTY_ID

and PRTY_RLTD.PRTY_RLTD_ROLE_CD=A.PRTY_RLTD_ROLE_CD

AND PRTY_RLTD.EDW_STRT_DTTM=A.EDW_STRT_DTTM

and PRTY_RLTD.TRANS_STRT_DTTM <>PRTY_RLTD.TRANS_END_DTTM

and lead is not null;





UPDATE  DB_T_PROD_CORE.PRTY_RLTD  
set TRANS_END_DTTM=  A.lead, EDW_END_DTTM=A.EDW_lead
FROM  

(

SELECT  distinct PRTY_ID,RLTD_PRTY_ID,EDW_STRT_DTTM,PRTY_RLTD_ROLE_CD,PRTY_RLTD_STRT_DTTM,

max(TRANS_STRT_DTTM) over (partition by RLTD_PRTY_ID,PRTY_RLTD_ROLE_CD  ORDER BY EDW_STRT_DTTM  ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND'' 

 as lead ,max(EDW_STRT_DTTM) over (partition by RLTD_PRTY_ID,PRTY_RLTD_ROLE_CD  ORDER BY EDW_STRT_DTTM  ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND'' 

 as EDW_lead

FROM DB_T_PROD_CORE.PRTY_RLTD   where  PRTY_RLTD_ROLE_CD in (''AGTPRDA'') and PRTY_ID <> ''9999''



)  A



where  PRTY_RLTD.RLTD_PRTY_ID = A.RLTD_PRTY_ID

and PRTY_RLTD.PRTY_ID=A.PRTY_ID

and PRTY_RLTD.PRTY_RLTD_ROLE_CD=A.PRTY_RLTD_ROLE_CD

AND PRTY_RLTD.EDW_STRT_DTTM=A.EDW_STRT_DTTM

and PRTY_RLTD.TRANS_STRT_DTTM <> PRTY_RLTD.TRANS_END_DTTM

and lead is not null;


INSERT INTO control_status (run_id, task_name, task_status, var_json)
SELECT :run_id, ''m_base_prty_rltd_insupd'', ''SUCCEEDED'', OBJECT_CONSTRUCT(
  ''StartTime'', :v_start_time,
  ''SrcSuccessRows'', (SELECT COUNT(*) FROM SQ_pc_prty_rltd) + (SELECT COUNT(*) FROM SQ_pc_prty_rltd_distadjuster) + (SELECT COUNT(*) FROM SQ_pc_prty_rltd_x_PRINS),
  ''TgtSuccessRows'', (
	(
		SELECT COUNT(*)
		FROM DB_T_PROD_CORE.PRTY_RLTD tgt
		JOIN EXPTRANS_upd_retired1 src
		ON tgt.PRTY_ID = src.lkp_PRTY_ID3
		AND tgt.RLTD_PRTY_ID = src.lkp_RLTD_PRTY_ID3
		AND tgt.PRTY_RLTD_ROLE_CD = src.lkp_PRTY_RLTD_ROLE_CD3
		AND tgt.EDW_STRT_DTTM = src.lkp_EDW_STRT_DTTM3
		WHERE
		(
			tgt.EDW_END_DTTM      IS DISTINCT FROM src.out_EDW_END_DTTM OR
			tgt.TRANS_END_DTTM    IS DISTINCT FROM src.Trans_strt_dttm4
		)
	) + 
	(SELECT COUNT(*) FROM exp_prty_rltd_insert1) + 
	(SELECT COUNT(*) FROM EXPTRANS_UPD_INS1) + 
	(
		SELECT COUNT(*)
		FROM DB_T_PROD_CORE.PRTY_RLTD tgt
		JOIN EXPTRANS1 src
		ON tgt.PRTY_ID = src.lkp_PRTY_ID3
		AND tgt.RLTD_PRTY_ID = src.lkp_RLTD_PRTY_ID3
		AND tgt.PRTY_RLTD_ROLE_CD = src.lkp_PRTY_RLTD_ROLE_CD3
		AND tgt.EDW_STRT_DTTM = src.lkp_EDW_STRT_DTTM3
		WHERE
		(
			tgt.EDW_END_DTTM   IS DISTINCT FROM src.Expiring_EndDate OR
			tgt.TRANS_END_DTTM IS DISTINCT FROM src.Trans_strt_dttm31
		)
	) + 
	(SELECT COUNT(*) FROM exp_prty_rltd_insert11) + 
	(
		SELECT COUNT(*)
		FROM DB_T_PROD_CORE.PRTY_RLTD tgt
		JOIN EXPTRANS11 src
		ON tgt.PRTY_ID = src.lkp_PRTY_ID3
		AND tgt.RLTD_PRTY_ID = src.lkp_RLTD_PRTY_ID3
		AND tgt.PRTY_RLTD_ROLE_CD = src.lkp_PRTY_RLTD_ROLE_CD3
		AND tgt.EDW_STRT_DTTM = src.lkp_EDW_STRT_DTTM3
		WHERE
			tgt.EDW_END_DTTM IS DISTINCT FROM src.Expiring_EndDate OR
			tgt.TRANS_END_DTTM IS DISTINCT FROM src.Trans_strt_dttm31
	) + 
	(
		SELECT COUNT(*) AS rows_to_update
		FROM DB_T_PROD_CORE.PRTY_RLTD tgt
		JOIN EXPTRANS_upd_retired11 src
		ON tgt.PRTY_ID = src.lkp_PRTY_ID3
		AND tgt.RLTD_PRTY_ID = src.lkp_RLTD_PRTY_ID3
		AND tgt.PRTY_RLTD_ROLE_CD = src.lkp_PRTY_RLTD_ROLE_CD3
		AND tgt.EDW_STRT_DTTM = src.lkp_EDW_STRT_DTTM3
		WHERE
			tgt.EDW_END_DTTM IS DISTINCT FROM src.out_EDW_END_DTTM OR
			tgt.TRANS_END_DTTM IS DISTINCT FROM src.Trans_strt_dttm4
	) + 
	(SELECT COUNT(*) FROM EXPTRANS_UPD_INS11) + 
	(SELECT COUNT(*) FROM exp_prty_rltd_insert) + 
	(
		SELECT COUNT(*) AS rows_to_update
		FROM DB_T_PROD_CORE.PRTY_RLTD tgt
		JOIN EXPTRANS_upd_retired src
		ON tgt.PRTY_ID = src.lkp_PRTY_ID3
		AND tgt.RLTD_PRTY_ID = src.lkp_RLTD_PRTY_ID3
		AND tgt.PRTY_RLTD_ROLE_CD = src.lkp_PRTY_RLTD_ROLE_CD3
		AND tgt.EDW_STRT_DTTM = src.lkp_EDW_STRT_DTTM3
		WHERE
			tgt.EDW_END_DTTM IS DISTINCT FROM src.out_EDW_END_DTTM OR
			tgt.TRANS_END_DTTM IS DISTINCT FROM src.Trans_strt_dttm4
	)
  )
);

EXCEPTION WHEN OTHER THEN
    INSERT INTO control_status (run_id, task_name, task_status, var_json)
    SELECT :run_id, ''m_base_prty_rltd_insupd'', ''FAILED'', OBJECT_CONSTRUCT(
        ''StartTime'', :v_start_time,
        ''SrcSuccessRows'', 0,
        ''TgtSuccessRows'', 0,
        ''SQLERRM'', :sqlerrm
    );

END; 
';