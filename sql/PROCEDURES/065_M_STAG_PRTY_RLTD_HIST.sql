-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_STAG_PRTY_RLTD_HIST("PARAM_JSON" VARCHAR)
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

/**District or Storm Center associated with the DB_T_CORE_PROD.adjuster relationship****/

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

	  rank() over (Partition by cu.AdjusterCode_Alfa_stg order by CASE WHEN right(cu.adjusterCode_alfa_stg,1)=  CASE WHEN cct.Name_stg = ''District'' then cg.name_stg

 END or left(cu.adjusterCode_alfa_stg,1)= CASE WHEN cct.Name_stg = ''District'' then cg.name_stg

 END  then 1 else 2 END) AS rankDistrict,

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

''PRTY_RLTD_ROLE25''  as Relationship, /*  Need to add value in XLAT table for DB_T_PROD_CORE.PRTY_RLTD_ROLE */
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

''PRTY_RLTD_ROLE27''  as Relationship, /*  Need to add value in XLAT table for DB_T_PROD_CORE.PRTY_RLTD_ROLE */
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

''PRTY_RLTD_ROLE28''  as Relationship, /*  Need to add value in XLAT table for DB_T_PROD_CORE.PRTY_RLTD_ROLE */
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

''PRTY_RLTD_ROLE25''  as Relationship, /*  Need to add value in XLAT table for DB_T_PROD_CORE.PRTY_RLTD_ROLE */
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





/*********************************** DB_T_STAG_DM_PROD.Claims Hierarchy END***********************************/



union



/* Agent to Producer Area */	



-- in DB_T_PROD_CORE.doc its 			pc_userproducercode/*  date not present****/												 */
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

-- AND UserRole.name_stg = ''Agent''	/* EIM - 36268 */															 */
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

/** Sales Hierarchy ALFA to DB_T_SHRD_PROD.State **/



-- in DB_T_PROD_CORE.doc its 			pc_parentgroup/*  date not present****/	 */


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

DB_T_PROD_STAG.pc_parentgroup ppg inner join DB_T_PROD_STAG.pc_group pg on pg.id_stg=ppg.ForeignEntityID_stg

inner join DB_T_PROD_STAG.pc_group child on child.id_stg=ppg.OwnerID_stg

inner join DB_T_PROD_STAG.pctl_grouptype pcg on pcg.id_stg=pg.GroupType_stg

inner join DB_T_PROD_STAG.pctl_grouptype childgrouptype on childgrouptype.id_stg=child.GroupType_stg

/************Marketing to District************/

inner join DB_T_PROD_STAG.pc_parentgroup pg_marketing_district on pg_marketing_district.ForeignEntityID_stg=ppg.OwnerID_stg

inner join DB_T_PROD_STAG.pc_group district on district.id_stg=pg_marketing_district.OwnerID_stg

inner join DB_T_PROD_STAG.pctl_grouptype district_type on district_type.id_stg=district.GroupType_stg

/********District to State*****************/

inner join DB_T_PROD_STAG.pc_groupregion pgr on pgr.GroupID_stg=pg_marketing_district.OwnerID_stg

inner join DB_T_PROD_STAG.pc_region_zone prz on pgr.RegionID_stg=prz.RegionId_stg

where 

(pcg.TYPECODE_stg=''root'' and childgrouptype.TYPECODE_stg=''region'') 



UNION



/** Sales Hierarchy DB_T_SHRD_PROD.State to Marketing **/

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

DB_T_PROD_STAG.pc_parentgroup ppg inner join DB_T_PROD_STAG.pc_group pg on pg.id_stg=ppg.ForeignEntityID_stg

inner join DB_T_PROD_STAG.pc_group child on child.id_stg=ppg.OwnerID_stg

inner join DB_T_PROD_STAG.pctl_grouptype pcg on pcg.id_stg=pg.GroupType_stg

inner join DB_T_PROD_STAG.pctl_grouptype childgrouptype on childgrouptype.id_stg=child.GroupType_stg

/************Marketing to District************/

inner join DB_T_PROD_STAG.pc_parentgroup pg_marketing_district on pg_marketing_district.ForeignEntityID_stg=ppg.OwnerID_stg

inner join DB_T_PROD_STAG.pc_group district on district.id_stg=pg_marketing_district.OwnerID_stg

inner join DB_T_PROD_STAG.pctl_grouptype district_type on district_type.id_stg=district.GroupType_stg

/********District to State*****************/

inner join DB_T_PROD_STAG.pc_groupregion pgr on pgr.GroupID_stg=pg_marketing_district.OwnerID_stg

inner join DB_T_PROD_STAG.pc_region_zone prz on pgr.RegionID_stg=prz.RegionId_stg

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

DB_T_PROD_STAG.pc_parentgroup ppg inner join DB_T_PROD_STAG.pc_group pg on pg.id_stg=ppg.ForeignEntityID_stg

inner join DB_T_PROD_STAG.pc_group child on child.id_stg=ppg.OwnerID_stg

inner join DB_T_PROD_STAG.pc_user pu on pu.id_stg=child.SupervisorID_stg

inner join DB_T_PROD_STAG.pc_contact pc on pc.id_stg=pu.ContactID_stg

inner join DB_T_PROD_STAG.pctl_grouptype pcg on pcg.id_stg=pg.GroupType_stg

inner join DB_T_PROD_STAG.pctl_grouptype childgrouptype on childgrouptype.id_stg=child.GroupType_stg

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

DB_T_PROD_STAG.pc_parentgroup ppg inner join DB_T_PROD_STAG.pc_group pg on pg.id_stg=ppg.ForeignEntityID_stg

inner join DB_T_PROD_STAG.pc_group child on child.id_stg=ppg.OwnerID_stg

inner join DB_T_PROD_STAG.pc_groupuser pgu on pgu.GroupID_stg=child.id_stg and pgu.Manager_stg=1

inner join DB_T_PROD_STAG.pc_user pu on pu.id_stg=pgu.UserID_stg 

 inner join DB_T_PROD_STAG.pc_contact pc on pc.id_stg=pu.ContactID_stg

inner join DB_T_PROD_STAG.pctl_grouptype pcg on pcg.id_stg=pg.GroupType_stg

inner join DB_T_PROD_STAG.pctl_grouptype childgrouptype on childgrouptype.id_stg=child.GroupType_stg



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

DB_T_PROD_STAG.pc_parentgroup ppg inner join DB_T_PROD_STAG.pc_group pg on pg.id_stg=ppg.ForeignEntityID_stg

inner join DB_T_PROD_STAG.pc_group child on child.id_stg=ppg.OwnerID_stg

inner join DB_T_PROD_STAG.pctl_grouptype pcg on pcg.id_stg=pg.GroupType_stg

  inner join DB_T_PROD_STAG.pctl_grouptype childgrouptype on childgrouptype.id_stg=child.GroupType_stg

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

DB_T_PROD_STAG.pc_parentgroup ppg inner join DB_T_PROD_STAG.pc_group pg on pg.id_stg=ppg.ForeignEntityID_stg

inner join DB_T_PROD_STAG.pc_group child on child.id_stg=ppg.OwnerID_stg

inner join DB_T_PROD_STAG.pctl_grouptype pcg on pcg.id_stg=pg.GroupType_stg

  inner join DB_T_PROD_STAG.pctl_grouptype childgrouptype on childgrouptype.id_stg=child.GroupType_stg

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

''PRTY_STRC_TYPE17'' AS Structure   --SLSPRSASSGN 
,(:start_dttm) as start_dttm,

(:end_dttm) as end_dttm

FROM DB_T_PROD_STAG.pc_producercode inner join DB_T_PROD_STAG.pc_userproducercode on pc_userproducercode.ProducerCodeID=pc_producercode.id

inner join  DB_T_PROD_STAG.pc_user on pc_user.id=pc_userproducercode.UserID 

 inner join DB_T_PROD_STAG.pc_contact on pc_contact.id=pc_user.ContactID

inner join DB_T_PROD_STAG.pc_userrole on pc_userrole.UserID=pc_user.ID

inner join DB_T_PROD_STAG.pc_role on pc_role.id=pc_userrole.RoleID

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

inner join DB_T_PROD_STAG.pctl_grouptype b on b.id_stg = a.GroupType_stg

inner join DB_T_PROD_STAG.pc_groupuser c on c.GroupID_stg = a.id_stg

inner join DB_T_PROD_STAG.pc_user d on d.id_stg = c.UserID_stg

inner join DB_T_PROD_STAG.pc_contact e on e.id_stg = d.ContactID_stg 

inner join DB_T_PROD_STAG.pc_userrole h on h.UserID_stg = d.id_stg

inner join DB_T_PROD_STAG.pc_role i on i.id_stg = h.RoleID_stg

where b.typecode_stg = ''servicecenter_alfa''


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
FROM DB_T_PROD_STAG.pc_producercode pp inner join DB_T_PROD_STAG.pc_userproducercode pup on pup.ProducerCodeID_stg=pp.id_stg

inner join  DB_T_PROD_STAG.pc_user pu on pu.id_stg=pup.UserID_stg

 inner join DB_T_PROD_STAG.pc_contact pc on pc.id_stg=pu.ContactID_stg



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
        ROW_NUMBER() OVER (PARTITION BY pp.Code_stg  ORDER BY pg.UpdateTime_stg DESC) AS RN 

from 

DB_T_PROD_STAG.pc_group pg 

 JOIN DB_T_PROD_STAG.pc_groupuser pgu on pg.ID_stg=pgu.GroupID_stg

JOIN  DB_T_PROD_STAG.pc_user pu on pgu.UserID_stg=pu.ID_stg

JOIN DB_T_PROD_STAG.pc_contact pc on pu.ContactID_stg=pc.ID_stg

JOIN DB_T_PROD_STAG.pctl_grouptype pcg on pg.GroupType_stg=pcg.ID_stg

--  JOIN DB_T_PROD_STAG.pctl_usertype pcu on pcu.ID_stg=pu.UserType_stg /*EIM -35544*/ */
LEFT JOIN DB_T_PROD_STAG.pc_userproducercode pup on pu.ID_stg=pup.UserID_stg

LEFT JOIN DB_T_PROD_STAG.pc_producercode pp on pup.ProducerCodeID_stg=pp.ID_stg

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

DB_T_PROD_STAG.pc_parentgroup ppg inner join DB_T_PROD_STAG.pc_group pg on pg.id_stg=ppg.ForeignEntityID_stg

inner join DB_T_PROD_STAG.pc_group child on child.id_stg=ppg.OwnerID_stg

inner join DB_T_PROD_STAG.pctl_grouptype pcg on pcg.id_stg=pg.GroupType_stg

  inner join DB_T_PROD_STAG.pctl_grouptype childgrouptype on childgrouptype.id_stg=child.GroupType_stg

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

DB_T_PROD_STAG.pc_parentgroup ppg inner join DB_T_PROD_STAG.pc_group pg on pg.id_stg=ppg.ForeignEntityID_stg

inner join DB_T_PROD_STAG.pc_group child on child.id_stg=ppg.OwnerID_stg

inner join DB_T_PROD_STAG.pctl_grouptype pcg on pcg.id_stg=pg.GroupType_stg

  inner join DB_T_PROD_STAG.pctl_grouptype childgrouptype on childgrouptype.id_stg=child.GroupType_stg

/** UW Office to UW District **/  

inner join DB_T_PROD_STAG.pc_parentgroup ho_to_district on ho_to_district.ForeignEntityID_stg=ppg.OwnerID_stg

inner join DB_T_PROD_STAG.pc_group district on district.id_stg=ho_to_district.OwnerID_stg

inner join DB_T_PROD_STAG.pctl_grouptype districttype on districttype.id_stg=district.GroupType_stg

/** Distrit to Region***/

inner join DB_T_PROD_STAG.pc_groupregion pgr on pgr.GroupID_stg=ho_to_district.OwnerID_stg

inner join DB_T_PROD_STAG.pc_region pr on pr.id_stg=pgr.RegionID_stg

/**Region to State**/

inner join DB_T_PROD_STAG.pc_region_zone prz on prz.RegionId_stg=pgr.RegionID_stg

where pcg.TYPECODE_stg in (''root'')

and childgrouptype.TYPECODE_stg in (''homeofficeuw'')

UNION

/**UW DB_T_SHRD_PROD.State to District****/

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

DB_T_PROD_STAG.pc_parentgroup ppg inner join DB_T_PROD_STAG.pc_group pg on pg.id_stg=ppg.ForeignEntityID_stg

inner join DB_T_PROD_STAG.pc_group child on child.id_stg=ppg.OwnerID_stg

inner join DB_T_PROD_STAG.pctl_grouptype pcg on pcg.id_stg=pg.GroupType_stg

  inner join DB_T_PROD_STAG.pctl_grouptype childgrouptype on childgrouptype.id_stg=child.GroupType_stg

/** UW Office to UW District **/  

inner join DB_T_PROD_STAG.pc_parentgroup ho_to_district on ho_to_district.ForeignEntityID_stg=ppg.OwnerID_stg

inner join DB_T_PROD_STAG.pc_group district on district.id_stg=ho_to_district.OwnerID_stg

inner join DB_T_PROD_STAG.pctl_grouptype districttype on districttype.id_stg=district.GroupType_stg

/** Distrit to Region***/

inner join DB_T_PROD_STAG.pc_groupregion pgr on pgr.GroupID_stg=ho_to_district.OwnerID_stg

inner join DB_T_PROD_STAG.pc_region pr on pr.id_stg=pgr.RegionID_stg

/**Region to State**/

inner join DB_T_PROD_STAG.pc_region_zone prz on prz.RegionId_stg=pgr.RegionID_stg

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

from DB_T_PROD_STAG.pc_groupuser pgu inner join DB_T_PROD_STAG.pc_group pg on pg.id_stg=pgu.GroupID_stg

inner join DB_T_PROD_STAG.pc_user pu on pu.id_stg=pgu.UserID_stg

inner join DB_T_PROD_STAG.pc_contact pc on pc.id_stg=pu.ContactID_stg

inner join DB_T_PROD_STAG.pctl_grouptype pcg on pcg.id_stg=pg.GroupType_stg

where pcg.TYPECODE_stg=''underwritingdistrict_alfa''

) as a where a.Relationship IN (''PRTY_RLTD_ROLE3'',''PRTY_RLTD_ROLE27'',''PRTY_RLTD_ROLE28'',''PRTY_RLTD_ROLE30'')



QUALIFY ROW_NUMBER() OVER (PARTITION BY PNI_AddressBookUID,OTHER_AddressBookUID,Relationship,Code,Name1,Lookups ORDER BY updatetime DESC)=1

 order by effectivedate asc,updatetime asc
) SRC
)
);


-- Component exp_Passthrough, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_Passthrough AS
(
SELECT
SQ_pc_prty_rltd_distadjuster.Relationship as Relationship,
SQ_pc_prty_rltd_distadjuster.PNI_AddressBookUID as PNI_AddressBookUID,
SQ_pc_prty_rltd_distadjuster.OTHER_AddressBookUID as OTHER_AddressBookUID,
SQ_pc_prty_rltd_distadjuster.Code as Code,
SQ_pc_prty_rltd_distadjuster.Name as Name1,
SQ_pc_prty_rltd_distadjuster.EffectiveDate as eff_dt,
SQ_pc_prty_rltd_distadjuster.ExpirationDate as end_dt,
SQ_pc_prty_rltd_distadjuster.Lookups as Lookups,
SQ_pc_prty_rltd_distadjuster.SYS_SRC_CD as SYS_SRC_CD,
SQ_pc_prty_rltd_distadjuster.updatetime as UpdateTime,
SQ_pc_prty_rltd_distadjuster.Retired as Retired,
SQ_pc_prty_rltd_distadjuster.Party_Structure_Type_Cd as Party_Structure_Type_Cd,
CURRENT_TIMESTAMP () as Load_dttm,
SQ_pc_prty_rltd_distadjuster.source_record_id
FROM
SQ_pc_prty_rltd_distadjuster
);


-- Component PRTY_RLTD_HIST, Type TARGET 
INSERT INTO DB_T_PROD_STAG.PRTY_RLTD_HIST
(
RELATIONSHIP,
PNI_ADDRESSBOOKUID,
OTHER_ADDRESSBOOKUID,
CODE,
NAME1,
EFF_DT,
END_DT,
LOOKUPS,
SYS_SRC_CD,
UPDATETIME,
RETIRED,
PARTY_STRUCTURE_TYPE_CD,
CUTOFF_DT
)
SELECT
exp_Passthrough.Relationship as RELATIONSHIP,
exp_Passthrough.PNI_AddressBookUID as PNI_ADDRESSBOOKUID,
exp_Passthrough.OTHER_AddressBookUID as OTHER_ADDRESSBOOKUID,
exp_Passthrough.Code as CODE,
exp_Passthrough.Name1 as NAME1,
exp_Passthrough.eff_dt as EFF_DT,
exp_Passthrough.end_dt as END_DT,
exp_Passthrough.Lookups as LOOKUPS,
exp_Passthrough.SYS_SRC_CD as SYS_SRC_CD,
exp_Passthrough.UpdateTime as UPDATETIME,
exp_Passthrough.Retired as RETIRED,
exp_Passthrough.Party_Structure_Type_Cd as PARTY_STRUCTURE_TYPE_CD,
exp_Passthrough.Load_dttm as CUTOFF_DT
FROM
exp_Passthrough;


END; ';