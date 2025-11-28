-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_BUSN_INSUPD("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' DECLARE start_dttm TIMESTAMP;
end_dttm TIMESTAMP;
PRCS_ID INTEGER;
FS_DATE date;
BEGIN 
start_dttm := CURRENT_TIMESTAMP();
end_dttm := CURRENT_TIMESTAMP();
PRCS_ID := 1;   

-- Component LKP_TERADATA_ETL_REF_XLAT, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM IN (''BUSN_CTGY'',''ORG_TYPE'',''PRTY_TYPE'')

	AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM IN (''derived'', ''cctl_contact.typecode'',''cctl_contact.name'',''abtl_abcontact.name'')

	AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS IN (''DS'',''GW'')

	AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_AGMT_SRC_CD, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_AGMT_SRC_CD AS
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


-- Component LKP_TERADATA_ETL_REF_XLAT_STS_CD, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_STS_CD AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''TAX_ID_STS'' 

	AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM IN (''cctl_taxstatus.typecode'',''derived'')

	AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS IN (''GW'' ,''DS'')

	AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_XREF_PRTY_BUSN, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_XREF_PRTY_BUSN AS
(
SELECT 
	DIR_PRTY.PRTY_ID as PRTY_ID, 
	DIR_PRTY.BUSN_CTGY_CD as BUSN_CTGY_CD, 
	DIR_PRTY.NK_BUSN_VAL as NK_BUSN_VAL
FROM 
	DB_T_PROD_CORE.DIR_PRTY
WHERE
	DIR_PRTY_VAL = ''BUSN''
);


-- Component sq_cctl_salvageyard_alfa, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_cctl_salvageyard_alfa AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as TYPECODE,
$2 as RETIRED,
$3 as SRC_IDNTFTN_VAL,
$4 as IncorporatedInd_alfa,
$5 as in_SYS_SRC_CD,
$6 as Busn_strt_dt,
$7 as TAX_ID_STS_CD,
$8 as updatetime,
$9 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select	typecode_stg,

		retired_stg, src_idntftn_val_stg,incorporatedind_alfa_stg,src_sys_cd_stg,

		busn_strt_dt_stg ,tax_id_sts_cd_stg,updatetime_stg  

from	(

        

	SELECT	DISTINCT

			cast(CASE 

				WHEN (MortgageeLienHolderNumber_alfa_stg IS NOT NULL 

		AND ab_abcontact.LinkID_stg NOT LIKE ''%MORT%''  

		AND ab_abcontact.LinkID_stg NOT LIKE ''%IRS%'' 

		AND  ab_abcontact.Source_stg = ''ContactManager'' )THEN MortgageeLienHolderNumber_alfa_stg 

		 WHEN (   ab_abcontact.Source_stg = ''ContactManager'' )THEN  ab_abcontact.LinkID_stg

		WHEN ab_abcontact.Source_stg = ''ClaimCenter'' THEN ab_abcontact.PublicID_stg

		ELSE ''UNK'' 

			END  as varchar(100))AS TYPECODE_stg,

		ab_abcontact.Retired_stg,

		TL_CNT_Name_stg AS SRC_IDNTFTN_VAL_stg,

/* case when IncorporatedInd_alfa_stg=1 then cast(0 as varchar(10)) */
/* else  */
IncorporatedInd_alfa_stg as IncorporatedInd_alfa_stg,

		''SRC_SYS7'' as SRC_SYS_CD_stg,

		ab_abcontact.createtime_stg as Busn_strt_dt_stg,

			ab_abcontact.TAX_ID_STS_CD_stg,

			ab_abcontact.updatetime_stg 

			

	FROM

	 (	

	/************* bc_contact*******************/

	select	cast(	''claimcenter'' as varchar(255)) as source_stg, 

	cast(null as varchar(255)) as mortgageelienholdernumber_alfa_stg,

		cast(null as varchar(10))  as incorporatedind_alfa_stg, 

		bc_contact.updatetime_stg,

	 	cast(bc_contact.publicid_stg as varchar(100)) as publicid_stg,

		cast(bc_contact.addressbookuid_stg as varchar(100))as linkid_stg,

		bc_contact.retired_stg, 

		bc_contact.createtime_stg,

		bctl_contact.name_stg as tl_cnt_name_stg,

		bctl_taxstatus.typecode_stg as tax_id_sts_cd_stg

	from

	DB_T_PROD_STAG.bc_contact 

		left outer join DB_T_PROD_STAG.bctl_contact 

		on bctl_contact.id_stg = bc_contact.subtype_stg 

		left outer join DB_T_PROD_STAG.bctl_gendertype 

		on bc_contact.gender_stg = bctl_gendertype.id_stg 

		left outer join DB_T_PROD_STAG.bctl_taxfilingstatustype	

		on bc_contact.taxfilingstatus_stg = bctl_taxfilingstatustype.id_stg

		left outer join DB_T_PROD_STAG.bctl_taxstatus 

		on bc_contact.taxstatus_stg = bctl_taxstatus.id_stg

		left outer join DB_T_PROD_STAG.bctl_maritalstatus 

		on bc_contact.maritalstatus_stg = bctl_maritalstatus.id_stg 

		left outer join DB_T_PROD_STAG.bctl_nameprefix 

		on bc_contact.prefix_stg = bctl_nameprefix.id_stg 

		left outer join DB_T_PROD_STAG.bctl_namesuffix 

		on bc_contact.suffix_stg = bctl_namesuffix.id_stg

		left outer join DB_T_PROD_STAG.bc_user 

		on bc_user.contactid_stg = bc_contact.id_stg

		left outer join DB_T_PROD_STAG.bc_credential 

		on bc_user.credentialid_stg = bc_credential.id_stg

	where

		bctl_contact.typecode_stg = (''usercontact'')  

		and 

/*  below condition added to avoid duplicates */
	       bc_contact.publicid_stg not in (''default_data:1'',

			''systemtables:1'',''systemtables:2'') 

		and ((bc_contact.updatetime_stg>(:start_dttm)	

		and bc_contact.updatetime_stg <=(:end_dttm)) 

		or (bc_user.updatetime_stg>(:start_dttm) 	

		and bc_user.updatetime_stg <= (:end_dttm)))

		

		

	union

	

	select	''claimcenter'' as source_stg ,

	cast(null as varchar(255)) as mortgageelienholdernumber_alfa_stg,

			cast(null as varchar(10))  as incorporatedind_alfa_stg, 

		 case when  bc_contact.updatetime_stg > a.updatetime_stg then  bc_contact.updatetime_stg else a.updatetime_stg end as updatetime_stg,

	 	 cast(case 

			when(bc_contact.externalid_stg is not null) then bc_contact.externalid_stg 

			else bc_contact.publicid_stg 

			end as varchar(100)) as publicid_stg,

			

			cast(bc_contact.addressbookuid_stg as varchar(100))as linkid_stg, 

			bc_contact.retired_stg,

			bc_contact.createtime_stg, 

			bctl_contact.name_stg as tl_cnt_name_stg,

			bctl_taxstatus.typecode_stg as tax_id_sts_cd_stg

		from	DB_T_PROD_STAG.bc_account a

	inner join DB_T_PROD_STAG.bc_accountcontact h 	

		on h.accountid_stg = a.id_stg

	inner join DB_T_PROD_STAG.bc_contact 

		on bc_contact.id_stg = h.contactid_stg

	join DB_T_PROD_STAG.bctl_contact 

		on bctl_contact.id_stg=bc_contact.subtype_stg

	left join DB_T_PROD_STAG.bc_accountcontactrole i 

		on i.accountcontactid_stg = h.id_stg

	left join DB_T_PROD_STAG.bctl_accountrole j 

		on j.id_stg = i.role_stg

	left outer join DB_T_PROD_STAG.bctl_gendertype 

		on bc_contact.gender_stg = bctl_gendertype.id_stg

		left outer join DB_T_PROD_STAG.bctl_taxfilingstatustype 

		on bc_contact.taxfilingstatus_stg = bctl_taxfilingstatustype.id_stg 

		left outer join DB_T_PROD_STAG.bctl_taxstatus 	

		on bc_contact.taxstatus_stg = bctl_taxstatus.id_stg

		left outer join DB_T_PROD_STAG.bctl_maritalstatus 	

		on bc_contact.maritalstatus_stg = bctl_maritalstatus.id_stg 

		left outer join DB_T_PROD_STAG.bctl_nameprefix 

		on bc_contact.prefix_stg = bctl_nameprefix.id_stg 

		left outer join DB_T_PROD_STAG.bctl_namesuffix 

		on bc_contact.suffix_stg = bctl_namesuffix.id_stg

		left outer join DB_T_PROD_STAG.bc_user 

		on bc_user.contactid_stg = bc_contact.id_stg

		left outer join DB_T_PROD_STAG.bc_credential 

		on bc_user.credentialid_stg = bc_credential.id_stg

	where	((h.primarypayer_stg = 1) 

		or (j.name_stg = ''payer''))

	and ((bc_contact.updatetime_stg>(:start_dttm) 

        and bc_contact.updatetime_stg <=(:end_dttm)) 

        or (bc_user.updatetime_stg>(:start_dttm)   

        and bc_user.updatetime_stg <= (:end_dttm))

        or (a.updatetime_stg>(:start_dttm)     

        and a.updatetime_stg <= (:end_dttm)))

		

		

	union

	

	select	''claimcenter'' as source_stg, 

	cast(null as varchar(255)) as mortgageelienholdernumber_alfa_stg,

		cast(null as varchar(10))  as incorporatedind_alfa, 

case when  bc_contact.updatetime_stg > a.updatetime_stg then  bc_contact.updatetime_stg else a.updatetime_stg end as updatetime_stg,

	 	cast(case 

				when (bc_contact.externalid_stg is null) then bc_contact.publicid_stg 

				else bc_contact.externalid_stg 

			end  as varchar(100))as publicid_stg, 

		cast(bc_contact.addressbookuid_stg as varchar(100)) as linkid_stg,

		bc_contact.retired_stg, 

		bc_contact.createtime_stg, 

		bctl_contact.name_stg as tl_cnt_name_stg,

		bctl_taxstatus.typecode_stg as tax_id_sts_cd_stg

	from	DB_T_PROD_STAG.bc_account a

	inner join DB_T_PROD_STAG.bc_invoicestream b 

		on a.id_stg = b.accountid_stg

	inner join DB_T_PROD_STAG.bc_accountcontact c 

		on c.accountid_stg=a.id_stg

	inner join DB_T_PROD_STAG.bc_contact 

		on bc_contact.id_stg = c.contactid_stg

	join DB_T_PROD_STAG.bctl_contact 

		on bctl_contact.id_stg=bc_contact.subtype_stg

	left join DB_T_PROD_STAG.bc_accountcontactrole f 

		on f.accountcontactid_stg = c.id_stg

	left join DB_T_PROD_STAG.bctl_accountrole g 

		on g.id_stg = f.role_stg

	left outer join DB_T_PROD_STAG.bctl_gendertype 

		on bc_contact.gender_stg = bctl_gendertype.id_stg 

		left outer join DB_T_PROD_STAG.bctl_taxfilingstatustype 

		on bc_contact.taxfilingstatus_stg = bctl_taxfilingstatustype.id_stg

		left outer join DB_T_PROD_STAG.bctl_taxstatus 

		on bc_contact.taxstatus_stg = bctl_taxstatus.id_stg

		left outer join DB_T_PROD_STAG.bctl_maritalstatus 

		on bc_contact.maritalstatus_stg = bctl_maritalstatus.id_stg 

		left outer join DB_T_PROD_STAG.bctl_nameprefix 

		on bc_contact.prefix_stg = bctl_nameprefix.id_stg 

		left outer join DB_T_PROD_STAG.bctl_namesuffix 

		on bc_contact.suffix_stg = bctl_namesuffix.id_stg

		left outer join DB_T_PROD_STAG.bc_user 

		on bc_user.contactid_stg = bc_contact.id_stg

		left outer join DB_T_PROD_STAG.bc_credential 

		on bc_user.credentialid_stg = bc_credential.id_stg

	where	((b.overridingpayer_alfa_stg is null 

		and c.primarypayer_stg = 1) 

		or (b.overridingpayer_alfa_stg is not null))

       and ((bc_contact.updatetime_stg>(:start_dttm) 

        and bc_contact.updatetime_stg <=(:end_dttm)) 

        or (bc_user.updatetime_stg>(:start_dttm)   

        and bc_user.updatetime_stg <= (:end_dttm))

        or (a.updatetime_stg>(:start_dttm)     

        and a.updatetime_stg <= (:end_dttm)))

	

	/************* pc_contact*******************/

	union

	

	select	''claimcenter'' as source_stg,  

	cast(null as varchar(255)) as mortgageelienholdernumber_alfa_stg,

		cast(null as varchar(10))  as incorporatedind_alfa_stg, 

		pc_contact.updatetime_stg,

	 	cast(pc_contact.publicid_stg as varchar(100)) as publicid_stg, 

		cast(pc_contact.addressbookuid_stg as varchar(100)) as linkid_stg,

		pc_contact.retired_stg, 

		pc_contact.createtime_stg, 

		pctl_contact.name_stg as tl_cnt_name_stg,

		pctl_taxstatus.typecode_stg as tax_id_sts_cd_stg

		from

		DB_T_PROD_STAG.pc_contact 

		left outer join DB_T_PROD_STAG.pctl_contact 

		on pctl_contact.id_stg = pc_contact.subtype_stg 

		left outer join DB_T_PROD_STAG.pctl_gendertype 

		on pc_contact.gender_stg = pctl_gendertype.id_stg

		left outer join DB_T_PROD_STAG.pctl_taxfilingstatustype 

		on pc_contact.taxfilingstatus_stg = pctl_taxfilingstatustype.id_stg 

		left outer join DB_T_PROD_STAG.pctl_taxstatus 

		on pc_contact.taxstatus_stg = pctl_taxstatus.id_stg

		left outer join DB_T_PROD_STAG.pctl_maritalstatus 

		on pc_contact.maritalstatus_stg = pctl_maritalstatus.id_stg 

		left outer join DB_T_PROD_STAG.pctl_nameprefix 

		on pc_contact.prefix_stg = pctl_nameprefix.id_stg 

		left outer join DB_T_PROD_STAG.pctl_namesuffix 

		on pc_contact.suffix_stg = pctl_namesuffix.id_stg

		left outer join DB_T_PROD_STAG.pc_user 

		on pc_user.contactid_stg = pc_contact.id_stg

		left outer join DB_T_PROD_STAG.pc_credential 

		on pc_user.credentialid_stg = pc_credential.id_stg

	left outer join DB_T_PROD_STAG.pc_policyperiod 

		on pc_policyperiod.pnicontactdenorm_stg = pc_contact.id_stg

	left outer join DB_T_PROD_STAG.pc_effectivedatedfields 

		on pc_effectivedatedfields.branchid_stg=pc_policyperiod.id_stg

	left outer join DB_T_PROD_STAG.pc_producercode 

		on pc_producercode.id_stg=pc_effectivedatedfields.producercodeid_stg

	where

		pctl_contact.typecode_stg = (''usercontact'')  

		and 

/*  below condition added to avoid duplicates */
	       pc_contact.publicid_stg not in (''default_data:1'',

			''systemtables:1'',''systemtables:2'') 

		 and ((pc_contact.updatetime_stg>(:start_dttm) 

		and pc_contact.updatetime_stg <= (:end_dttm)) 

		or (pc_user.updatetime_stg>(:start_dttm) 

		and pc_user.updatetime_stg <= (:end_dttm)))

		union 

		/*********************************** DB_T_PROD_STAG.cc_contact	****************************************/

		

		select	distinct	''claimcenter'' as source_stg, 

		cast(null as varchar(255)) as mortgageelienholdernumber_alfa_stg,

			 

		cast(cc_contact.incorporatedind_alfa_stg as varchar(10)) as incorporatedind_alfa_stg, 

		cc_contact.updatetime_stg,

			

		cast(cc_contact.publicid_stg as varchar(100)) as publicid_stg, 

		cast(cc_contact.addressbookuid_stg as varchar(100)) as linkid_stg,

			

		cc_contact.retired_stg, 

		cc_contact.createtime_stg, 

		cctl_contact.NAME_stg AS TL_CNT_Name_stg,

		cctl_taxstatus.typecode_stg AS TAX_ID_STS_CD_stg

		from

		DB_T_PROD_STAG.cc_contact 

		left outer join DB_T_PROD_STAG.cctl_contact 

		on cctl_contact.id_stg = cc_contact.subtype_stg 

		left outer join DB_T_PROD_STAG.cctl_gendertype 

		on cc_contact.gender_stg = cctl_gendertype.id_stg 

		left outer join DB_T_PROD_STAG.cctl_taxfilingstatustype 

		on cc_contact.taxfilingstatus_stg = cctl_taxfilingstatustype.id_stg 

		left outer join DB_T_PROD_STAG.cctl_taxstatus 

		on cc_contact.taxstatus_stg = cctl_taxstatus.id_stg

		left outer join DB_T_PROD_STAG.cctl_maritalstatus 

		on cc_contact.maritalstatus_stg = cctl_maritalstatus.id_stg 

		left outer join DB_T_PROD_STAG.cctl_nameprefix 

		on cc_contact.prefix_stg = cctl_nameprefix.id_stg 

		left outer join DB_T_PROD_STAG.cctl_namesuffix 

		on cc_contact.suffix_stg = cctl_namesuffix.id_stg

		left outer join DB_T_PROD_STAG.cc_user 

		on cc_user.contactid_stg = cc_contact.id_stg

		left outer join DB_T_PROD_STAG.cc_credential 

		on cc_user.credentialid_stg = cc_credential.id_stg

		left outer join DB_T_PROD_STAG.cc_claimcontact 

		on cc_contact.id_stg=cc_claimcontact.contactid_stg

		left outer join DB_T_PROD_STAG.cc_claimcontactrole 

		on cc_claimcontact.id_stg=cc_claimcontactrole.claimcontactid_stg

		left outer join DB_T_PROD_STAG.cc_incident 

		on cc_claimcontactrole.claimcontactid_stg =cc_incident.id_stg

	where

		(cc_contact.updatetime_stg>(:start_dttm) 

		and cc_contact.updatetime_stg <= (:end_dttm) )  

		or   (cc_user.updatetime_stg>(:start_dttm) 

		and cc_user.updatetime_stg <=(:end_dttm) )

		

		union

	/********************************************* ab_contact*********************************/	

		

	select	''ContactManager'' as source_stg, 

		ab_abcontact.mortgageelienholdernumber_alfa_stg,

	  	cast (ab_abcontact.IncorporatedInd_alfa_stg as varchar(10)) as incorporatedind_alfa_stg,

			

		ab_abcontact.UpdateTime_stg, 

		cast(null as varchar(100)) as PublicID_stg,

		cast(ab_abcontact.LinkID_stg as varchar(100))  as LinkID_stg, 

		ab_abcontact.Retired_stg, 

		ab_abcontact.CreateTime_stg,

	 	abtl_abcontact.NAME_stg AS TL_CNT_Name_stg,

		abtl_taxstatus.typecode_stg AS TAX_ID_STS_CD_stg

		FROM		DB_T_PROD_STAG.ab_abcontact

		LEFT OUTER JOIN DB_T_PROD_STAG.abtl_abcontact 

		ON abtl_abcontact.id_stg = ab_abcontact.subtype_stg 

		LEFT OUTER JOIN DB_T_PROD_STAG.abtl_gendertype 

		ON ab_abcontact.gender_stg = abtl_gendertype.id_stg 

		LEFT OUTER JOIN DB_T_PROD_STAG.abtl_taxfilingstatustype 

		ON ab_abcontact.TaxFilingStatus_stg = abtl_taxfilingstatustype.id_stg 

		LEFT OUTER JOIN DB_T_PROD_STAG.abtl_taxstatus 

		ON ab_abcontact.taxstatus_stg = abtl_taxstatus.id_stg

		LEFT OUTER JOIN DB_T_PROD_STAG.abtl_maritalstatus 

		ON ab_abcontact.Maritalstatus_stg = abtl_maritalstatus.id_stg

		LEFT OUTER JOIN DB_T_PROD_STAG.abtl_nameprefix 

		ON ab_abcontact.prefix_stg = abtl_nameprefix.id_stg 

		LEFT OUTER JOIN DB_T_PROD_STAG.abtl_namesuffix 

		ON ab_abcontact.Suffix_stg = abtl_namesuffix.id_stg

		LEFT OUTER JOIN DB_T_PROD_STAG.ab_user 

		ON ab_user.ContactID_stg = ab_abcontact.id_stg

		LEFT OUTER JOIN DB_T_PROD_STAG.ab_credential 

		on ab_user.CredentialID_stg = ab_credential.id_stg

		LEFT OUTER JOIN DB_T_PROD_STAG.abtl_occupation 

		ON  abtl_occupation.ID_stg = ab_abcontact.occupation_alfa_stg

	WHERE	ab_abcontact.UpdateTime_stg>(:start_dttm) 

		AND ab_abcontact.UpdateTime_stg <= (:end_dttm)

	 )

	 ab_abcontact 

	WHERE

/* TL_CNT_Name in (''Company'',''CompanyVendor'',''AutoRepairShop'',''AutoTowingAgcy'',''LawFirm'', ''MedicalCareOrg'') */
	 	

	 	TL_CNT_Name_stg in (''Company'',

			''Vendor (Company)'',''Auto Repair Shop'',''Auto Towing Agcy'',''Law Firm'',

			''Medical Care Organization'', ''Lodging (Company)'',''Lodging Provider (Org)'') 

		 

			 

			 

	

	union 

	

	select	distinct

		cast(upper(cctl_salvageyard_alfa.typecode_stg) as varchar(100)) as typecode_stg, 

			

		cctl_salvageyard_alfa.retired_stg,

		''salvg'' as src_idntftn_val_stg,

		cast(null as varchar(10))  as incorporatedind_alfa_stg,

		''src_sys6'' as src_sys_cd_stg,

		cc_incident.createtime_stg as busn_strt_dt_stg	,

		cast(null as varchar (90) )as tax_id_sts_cd_stg,

		cc_incident.updatetime_stg

	from

	( 

		select	distinct 

				cc_incident.updatetime_stg,

				cc_incident.createtime_stg,

				cc_incident.salvageyard_alfa_stg 

		from

		DB_T_PROD_STAG.cc_incident 

		inner join (

					select	cc_claim.* 		

			from	DB_T_PROD_STAG.cc_claim 	

					inner join DB_T_PROD_STAG.cctl_claimstate 

					on cc_claim.state_stg= cctl_claimstate.id_stg 

					where	cctl_claimstate.name_stg <> ''draft'') cc_claim 

			on cc_claim.id_stg=cc_incident.claimid_stg

		left outer join DB_T_PROD_STAG.cc_vehicle 

			on cc_incident.vehicleid_stg=cc_vehicle.id_stg

		left outer join DB_T_PROD_STAG.cc_injurydiagnosis 

			on cc_incident.id_stg=cc_injurydiagnosis.injuryincidentid_stg

		left outer join DB_T_PROD_STAG.cc_icdcode 

			on cc_injurydiagnosis.icdcode_stg=cc_icdcode.id_stg

		left outer join DB_T_PROD_STAG.cctl_icdbodysystem 

			on  cctl_icdbodysystem.id_stg=cc_icdcode.bodysystem_stg

		/*left outer join DB_T_PROD_STAG.cctl_losspartytype 

			on cctl_losspartytype.id_stg = cc_incident.vehiclelossparty_stg*/

		left outer join DB_T_PROD_STAG.cc_address 

			on cc_claim.losslocationid_stg = cc_address.id_stg

		left outer join DB_T_PROD_STAG.cc_policylocation 

			on cc_policylocation.addressid_stg= cc_address.id_stg

		left outer join DB_T_PROD_STAG.cc_riskunit 

			on cc_riskunit.policylocationid_stg =cc_policylocation.id_stg

		left outer join DB_T_PROD_STAG.cctl_constructiontype_alfa 

			on cc_riskunit.constructiontype_alfa_stg=cctl_constructiontype_alfa.id_stg

		left outer join DB_T_PROD_STAG.cc_claimcontactrole 

			on cc_incident.id_stg = cc_claimcontactrole.incidentid_stg

		left outer join DB_T_PROD_STAG.cc_claimcontact 

			on cc_claimcontactrole.claimcontactid_stg=cc_claimcontact.id_stg

		left outer join DB_T_PROD_STAG.cc_contact 

			on cc_claimcontact.contactid_stg=cc_contact.id_stg

		where	

		cc_incident.updatetime_stg > (:start_dttm)

			and cc_incident.updatetime_stg <= (:end_dttm) )	cc_incident join DB_T_PROD_STAG.cctl_salvageyard_alfa    

		on cc_incident.salvageyard_alfa_stg=cctl_salvageyard_alfa.id_stg

	qualify	row_number () over (

	partition by cctl_salvageyard_alfa.typecode_stg 

	order by cc_incident.updatetime_stg desc )=1

	union

	

	 select	distinct 

	 	upper(pctl_priorcarrier_alfa.typecode_stg) as typecode_stg, 

			

	 	pctl_priorcarrier_alfa.retired_stg,

	 	''Insurance Carrier'' as src_idntftn_val_stg,

	 	cast(null as varchar(10)) as incorporatedind_alfa_stg,

		''src_sys4'' as src_sys_cd_stg,

		cast (''1900-01-01'' as date) as busn_strt_dt_stg,

		cast(null as varchar (90)) as tax_id_sts_cd_stg,

		pc_eff.updatetime_stg

			

	from

	 	DB_T_PROD_STAG.pctl_priorcarrier_alfa   

	left outer join ( 

		select	distinct  effdt.updatetime_stg as updatetime_stg,effdt.priorcarrier_alfa_stg as priorcarrier_alfa_stg

		from	DB_T_PROD_STAG.pc_effectivedatedfields as effdt

		left outer join DB_T_PROD_STAG.pc_policyperiod pp 

			on pp.id_stg=effdt.branchid_stg

		left outer join DB_T_PROD_STAG.pc_policycontactrole pcr 

			on pp.id_stg=pcr.branchid_stg 

			and pcr.fixedid_stg=effdt.primarynamedinsured_stg

		left outer join DB_T_PROD_STAG.pc_contact cnt 

			on cnt.id_stg=pcr.contactdenorm_stg

		left outer join  DB_T_PROD_STAG.pctl_contact 

			on pctl_contact.id_stg = cnt.subtype_stg 

		left outer join DB_T_PROD_STAG.pc_job pcj 

			on pcj.id_stg = pp.jobid_stg

		 left outer  join DB_T_PROD_STAG.pctl_job pctlj 

			on pctlj.id_stg=pcj.subtype_stg

		left join DB_T_PROD_STAG.pctl_policyperiodstatus pps 

			on pps.id_stg = pp.status_stg 

		where	effdt.expirationdate_stg is null 

			and pctlj.typecode_stg  in (''Cancellation'',''PolicyChange'',''Reinstatement'',

		''Renewal'',''Rewrite'',''Submission'')

		 and pps.typecode_stg<>''Temporary''  

			and effdt.updatetime_stg > (:start_dttm) 

			and effdt.updatetime_stg <= (:end_dttm) )pc_eff   

		on pc_eff.priorcarrier_alfa_stg=pctl_priorcarrier_alfa.id_stg

	 	qualify	row_number () over (

	partition by pctl_priorcarrier_alfa.typecode_stg 

	order by pc_eff.updatetime_stg desc )=1

	 	 ) as a    

 qualify	row_number() over(

partition by typecode_stg,src_idntftn_val_stg,incorporatedind_alfa_stg,

		src_sys_cd_stg,tax_id_sts_cd_stg

order by updatetime_stg desc) = 1

 order by typecode_stg,

		busn_strt_dt_stg asc nulls first
) SRC
)
);


-- Component exp_all_source, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_all_source AS
(
SELECT
sq_cctl_salvageyard_alfa.TYPECODE as TYPECODE,
sq_cctl_salvageyard_alfa.RETIRED as RETIRED,
TO_DATE ( ''1900-01-01'' , ''yyyy-mm-dd'' ) as var_def_strt_dt,
CASE
  WHEN sq_cctl_salvageyard_alfa.SRC_IDNTFTN_VAL = ''Insurance Carrier'' THEN LKP_1.TGT_IDNTFTN_VAL
  WHEN sq_cctl_salvageyard_alfa.SRC_IDNTFTN_VAL = ''SALVG'' THEN LKP_2.TGT_IDNTFTN_VAL
  WHEN NOT sq_cctl_salvageyard_alfa.SRC_IDNTFTN_VAL IN (''SALVG'', ''Insurance Carrier'') THEN LKP_3.TGT_IDNTFTN_VAL
  ELSE ''UNK''
END AS var_BUSN_CTGY_CD,
var_BUSN_CTGY_CD as out_BUSN_CTGY_CD,
LKP_4.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT */ as out_ORG_TYPE_CD,
TO_TIMESTAMP ( ''9999-12-31 23:59:59.999999'' , ''yyyy-mm-dd HH24:MI:SS.FF6'' ) as out_BUSN_END_DT,
LKP_5.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT */ as out_PRTY_TYPE_CD,
''UNK'' as out_TAX_BRAKT_CD,
''UNK'' as out_GICS_SBIDSTRY_CD,
''UNK'' as out_LIFCYCL_CD,
:PRCS_ID as out_PRCS_ID,
sq_cctl_salvageyard_alfa.IncorporatedInd_alfa as IncorporatedInd_alfa,
CURRENT_TIMESTAMP as EDW_STRT_DTTM,
TO_TIMESTAMP ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as EDW_END_DTTM,
LKP_6.PRTY_ID /* replaced lookup LKP_XREF_PRTY_BUSN */ as BUSN_PRTY_ID,
LKP_7.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_AGMT_SRC_CD */ as out_SYS_SRC_CD,
CASE WHEN sq_cctl_salvageyard_alfa.Busn_strt_dt IS NULL THEN var_def_strt_dt ELSE sq_cctl_salvageyard_alfa.Busn_strt_dt END as out_Busn_strt_dt,
DECODE ( TRUE , LKP_8.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_STS_CD */ IS NULL , ''UNK'' , LKP_9.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_STS_CD */ ) as OUT_TAX_ID_STS_CD,
CASE WHEN sq_cctl_salvageyard_alfa.updatetime IS NULL THEN var_def_strt_dt ELSE sq_cctl_salvageyard_alfa.updatetime END as out_Trans_strt_dt,
sq_cctl_salvageyard_alfa.source_record_id,
row_number() over (partition by sq_cctl_salvageyard_alfa.source_record_id order by sq_cctl_salvageyard_alfa.source_record_id) as RNK
FROM
sq_cctl_salvageyard_alfa
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = ''BUSN_CTGY6''
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = ''BUSN_CTGY5''
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT LKP_3 ON LKP_3.SRC_IDNTFTN_VAL = sq_cctl_salvageyard_alfa.SRC_IDNTFTN_VAL
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT LKP_4 ON LKP_4.SRC_IDNTFTN_VAL = ''ORG_TYPE1''
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT LKP_5 ON LKP_5.SRC_IDNTFTN_VAL = ''PRTY_TYPE2''
LEFT JOIN LKP_XREF_PRTY_BUSN LKP_6 ON LKP_6.BUSN_CTGY_CD = var_BUSN_CTGY_CD AND LKP_6.NK_BUSN_VAL = sq_cctl_salvageyard_alfa.TYPECODE
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_AGMT_SRC_CD LKP_7 ON LKP_7.SRC_IDNTFTN_VAL = sq_cctl_salvageyard_alfa.in_SYS_SRC_CD
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_STS_CD LKP_8 ON LKP_8.SRC_IDNTFTN_VAL = sq_cctl_salvageyard_alfa.TAX_ID_STS_CD
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_STS_CD LKP_9 ON LKP_9.SRC_IDNTFTN_VAL = sq_cctl_salvageyard_alfa.TAX_ID_STS_CD
QUALIFY RNK = 1
);


-- Component LKP_BUSN_TGT, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_BUSN_TGT AS
(
SELECT
LKP.BUSN_STRT_DTTM,
LKP.BUSN_CTGY_CD,
LKP.TAX_BRAKT_CD,
LKP.ORG_TYPE_CD,
LKP.GICS_SBIDSTRY_CD,
LKP.BUSN_END_DTTM,
LKP.LIFCYCL_CD,
LKP.PRTY_TYPE_CD,
LKP.NK_BUSN_CD,
LKP.EDW_STRT_DTTM,
LKP.EDW_END_DTTM,
LKP.SRC_SYS_CD,
LKP.TAX_ID_STS_CD,
LKP.INC_IND,
exp_all_source.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_all_source.source_record_id ORDER BY LKP.BUSN_STRT_DTTM asc,LKP.BUSN_CTGY_CD asc,LKP.TAX_BRAKT_CD asc,LKP.ORG_TYPE_CD asc,LKP.GICS_SBIDSTRY_CD asc,LKP.BUSN_END_DTTM asc,LKP.LIFCYCL_CD asc,LKP.PRTY_TYPE_CD asc,LKP.NK_BUSN_CD asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc,LKP.SRC_SYS_CD asc,LKP.TAX_ID_STS_CD asc,LKP.INC_IND asc) RNK
FROM
exp_all_source
LEFT JOIN (
SELECT	BUSN.BUSN_STRT_DTTM as BUSN_STRT_DTTM,
		BUSN.TAX_BRAKT_CD as TAX_BRAKT_CD, 
		BUSN.ORG_TYPE_CD as ORG_TYPE_CD,
		BUSN.GICS_SBIDSTRY_CD as GICS_SBIDSTRY_CD, 
		BUSN.BUSN_END_DTTM as BUSN_END_DTTM,
		BUSN.LIFCYCL_CD as LIFCYCL_CD, 
		BUSN.PRTY_TYPE_CD as PRTY_TYPE_CD,
		BUSN.EDW_STRT_DTTM as EDW_STRT_DTTM, 
		BUSN.EDW_END_DTTM as EDW_END_DTTM,
        COALESCE(BUSN.SRC_SYS_CD, '' '') AS SRC_SYS_CD,
		--BUSN.SRC_SYS_CD as SRC_SYS_CD, 
		BUSN.TAX_ID_STS_CD as TAX_ID_STS_CD,
		BUSN.NK_BUSN_CD as NK_BUSN_CD, 
		BUSN.BUSN_CTGY_CD as BUSN_CTGY_CD ,
	      BUSN.INC_IND as INC_IND 
FROM	DB_T_PROD_CORE.BUSN
QUALIFY	ROW_NUMBER () OVER (
PARTITION BY NK_BUSN_CD,BUSN_CTGY_CD 
ORDER BY EDW_END_DTTM DESC )=1
) LKP ON LKP.NK_BUSN_CD = exp_all_source.TYPECODE AND LKP.BUSN_CTGY_CD = exp_all_source.out_BUSN_CTGY_CD
QUALIFY RNK = 1
);


-- Component exp_insert_update_flag, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_insert_update_flag AS
(
SELECT
LKP_BUSN_TGT.BUSN_CTGY_CD as lkp_BUSN_CTGY_CD,
LKP_BUSN_TGT.NK_BUSN_CD as lkp_NK_BUSN_CD,
LKP_BUSN_TGT.SRC_SYS_CD as lkp_SYS_SRC_CD,
LKP_BUSN_TGT.ORG_TYPE_CD as lkp_ORG_TYPE_CD,
LKP_BUSN_TGT.PRTY_TYPE_CD as lkp_PRTY_TYPE_CD,
LKP_BUSN_TGT.BUSN_END_DTTM as lkp_BUSN_END_DT,
LKP_BUSN_TGT.BUSN_STRT_DTTM as lkp_BUSN_STRT_DT,
LKP_BUSN_TGT.EDW_STRT_DTTM as lkp_EDW_STRT_DT,
LKP_BUSN_TGT.EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_all_source.BUSN_PRTY_ID as in_BUSN_PRTY_ID,
exp_all_source.out_Busn_strt_dt as in_BUSN_STRT_DT,
exp_all_source.out_BUSN_CTGY_CD as in_BUSN_CTGY_CD,
exp_all_source.out_TAX_BRAKT_CD as in_TAX_BRAKT_CD,
exp_all_source.out_ORG_TYPE_CD as in_ORG_TYPE_CD,
exp_all_source.out_GICS_SBIDSTRY_CD as in_GICS_SBIDSTRY_CD,
exp_all_source.out_BUSN_END_DT as in_BUSN_END_DT,
exp_all_source.out_LIFCYCL_CD as in_LIFCYCL_CD,
exp_all_source.out_PRTY_TYPE_CD as in_PRTY_TYPE_CD,
exp_all_source.TYPECODE as in_NK_BUSN_CD,
exp_all_source.IncorporatedInd_alfa as in_INC_IND,
exp_all_source.out_PRCS_ID as in_PRCS_ID,
exp_all_source.EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_all_source.EDW_END_DTTM as in_EDW_END_DTTM,
exp_all_source.out_SYS_SRC_CD as in_SYS_SRC_CD,
MD5 ( LKP_BUSN_TGT.TAX_BRAKT_CD || LKP_BUSN_TGT.ORG_TYPE_CD || LKP_BUSN_TGT.GICS_SBIDSTRY_CD || LKP_BUSN_TGT.LIFCYCL_CD || LKP_BUSN_TGT.PRTY_TYPE_CD || TO_CHAR ( LKP_BUSN_TGT.BUSN_STRT_DTTM , ''YYYY-MM-DD'' ) || LKP_BUSN_TGT.TAX_ID_STS_CD || LKP_BUSN_TGT.INC_IND ) as v_lkp_MD5,
MD5 ( exp_all_source.out_TAX_BRAKT_CD || exp_all_source.out_ORG_TYPE_CD || exp_all_source.out_GICS_SBIDSTRY_CD || exp_all_source.out_LIFCYCL_CD || exp_all_source.out_PRTY_TYPE_CD || TO_CHAR ( exp_all_source.out_Busn_strt_dt , ''YYYY-MM-DD'' ) || exp_all_source.OUT_TAX_ID_STS_CD || exp_all_source.IncorporatedInd_alfa ) as v_in_MD5,
exp_all_source.OUT_TAX_ID_STS_CD as in_TAX_ID_STS_CD,
NULL as NewLookupRow,
CASE WHEN LKP_BUSN_TGT.BUSN_CTGY_CD IS NULL THEN 1 ELSE 0 END as InsertFlag,
CASE WHEN LKP_BUSN_TGT.BUSN_CTGY_CD IS NOT NULL AND v_lkp_MD5 <> v_in_MD5 THEN 1 ELSE 0 END as UpdateFlag,
CASE WHEN LKP_BUSN_TGT.BUSN_CTGY_CD IS NOT NULL AND v_lkp_MD5 = v_in_MD5 THEN 1 ELSE 0 END as RetireFlag,
CASE WHEN v_lkp_MD5 IS NULL THEN ''I'' ELSE ( CASE WHEN v_lkp_MD5 <> v_in_MD5 THEN ''U'' ELSE ''R'' END ) END as InsUpdaFlag,
exp_all_source.RETIRED as RETIRED,
exp_all_source.out_Trans_strt_dt as updatetime,
exp_all_source.source_record_id
FROM
exp_all_source
INNER JOIN LKP_BUSN_TGT ON exp_all_source.source_record_id = LKP_BUSN_TGT.source_record_id
);


-- Component rtr_insert_update_flag_insert, Type ROUTER Output Group insert
CREATE OR REPLACE TEMPORARY TABLE rtr_insert_update_flag_insert AS
(SELECT
exp_insert_update_flag.InsertFlag as InsertFlag,
exp_insert_update_flag.UpdateFlag as UpdateFlag,
exp_insert_update_flag.lkp_BUSN_CTGY_CD as lkp_BUSN_CTGY_CD,
exp_insert_update_flag.lkp_NK_BUSN_CD as lkp_NK_BUSN_CD,
exp_insert_update_flag.lkp_SYS_SRC_CD as lkp_SYS_SRC_CD,
exp_insert_update_flag.lkp_EDW_STRT_DT as lkp_EDW_STRT_DT,
exp_insert_update_flag.in_BUSN_PRTY_ID as in_BUSN_PRTY_ID,
exp_insert_update_flag.in_BUSN_STRT_DT as in_BUSN_STRT_DT,
exp_insert_update_flag.in_BUSN_CTGY_CD as in_BUSN_CTGY_CD,
exp_insert_update_flag.in_TAX_BRAKT_CD as in_TAX_BRAKT_CD,
exp_insert_update_flag.in_ORG_TYPE_CD as in_ORG_TYPE_CD,
exp_insert_update_flag.in_GICS_SBIDSTRY_CD as in_GICS_SBIDSTRY_CD,
exp_insert_update_flag.in_BUSN_END_DT as in_BUSN_END_DT,
exp_insert_update_flag.in_LIFCYCL_CD as in_LIFCYCL_CD,
exp_insert_update_flag.in_PRTY_TYPE_CD as in_PRTY_TYPE_CD,
exp_insert_update_flag.in_NK_BUSN_CD as in_NK_BUSN_CD,
exp_insert_update_flag.in_INC_IND as in_INC_IND,
exp_insert_update_flag.in_PRCS_ID as in_PRCS_ID,
exp_insert_update_flag.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_insert_update_flag.in_EDW_END_DTTM as in_EDW_END_DTTM,
exp_insert_update_flag.in_SYS_SRC_CD as in_SYS_SRC_CD,
exp_insert_update_flag.in_TAX_ID_STS_CD as in_TAX_ID_STS_CD,
exp_insert_update_flag.NewLookupRow as Flag,
exp_insert_update_flag.lkp_PRTY_TYPE_CD as lkp_PRTY_TYPE_CD,
exp_insert_update_flag.lkp_BUSN_END_DT as lkp_BUSN_END_DT,
exp_insert_update_flag.lkp_ORG_TYPE_CD as lkp_ORG_TYPE_CD4,
exp_insert_update_flag.lkp_BUSN_STRT_DT as lkp_BUSN_STRT_DT,
exp_insert_update_flag.RetireFlag as RetireFlag,
exp_insert_update_flag.RETIRED as RETIRED,
exp_insert_update_flag.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_insert_update_flag.InsUpdaFlag as InsUpdaFlag,
exp_insert_update_flag.updatetime as updatetime,
exp_insert_update_flag.source_record_id
FROM
exp_insert_update_flag
WHERE exp_insert_update_flag.in_BUSN_PRTY_ID IS NOT NULL and ( exp_insert_update_flag.InsUpdaFlag = ''I'' OR exp_insert_update_flag.InsUpdaFlag = ''U'' OR ( exp_insert_update_flag.RETIRED = 0 and exp_insert_update_flag.lkp_EDW_END_DTTM != TO_TIMESTAMP ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ) ));


-- Component rtr_insert_update_flag_retire, Type ROUTER Output Group retire
CREATE OR REPLACE TEMPORARY TABLE rtr_insert_update_flag_retire AS
(SELECT
exp_insert_update_flag.InsertFlag as InsertFlag,
exp_insert_update_flag.UpdateFlag as UpdateFlag,
exp_insert_update_flag.lkp_BUSN_CTGY_CD as lkp_BUSN_CTGY_CD,
exp_insert_update_flag.lkp_NK_BUSN_CD as lkp_NK_BUSN_CD,
exp_insert_update_flag.lkp_SYS_SRC_CD as lkp_SYS_SRC_CD,
exp_insert_update_flag.lkp_EDW_STRT_DT as lkp_EDW_STRT_DT,
exp_insert_update_flag.in_BUSN_PRTY_ID as in_BUSN_PRTY_ID,
exp_insert_update_flag.in_BUSN_STRT_DT as in_BUSN_STRT_DT,
exp_insert_update_flag.in_BUSN_CTGY_CD as in_BUSN_CTGY_CD,
exp_insert_update_flag.in_TAX_BRAKT_CD as in_TAX_BRAKT_CD,
exp_insert_update_flag.in_ORG_TYPE_CD as in_ORG_TYPE_CD,
exp_insert_update_flag.in_GICS_SBIDSTRY_CD as in_GICS_SBIDSTRY_CD,
exp_insert_update_flag.in_BUSN_END_DT as in_BUSN_END_DT,
exp_insert_update_flag.in_LIFCYCL_CD as in_LIFCYCL_CD,
exp_insert_update_flag.in_PRTY_TYPE_CD as in_PRTY_TYPE_CD,
exp_insert_update_flag.in_NK_BUSN_CD as in_NK_BUSN_CD,
exp_insert_update_flag.in_INC_IND as in_INC_IND,
exp_insert_update_flag.in_PRCS_ID as in_PRCS_ID,
exp_insert_update_flag.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_insert_update_flag.in_EDW_END_DTTM as in_EDW_END_DTTM,
exp_insert_update_flag.in_SYS_SRC_CD as in_SYS_SRC_CD,
exp_insert_update_flag.in_TAX_ID_STS_CD as in_TAX_ID_STS_CD,
exp_insert_update_flag.NewLookupRow as Flag,
exp_insert_update_flag.lkp_PRTY_TYPE_CD as lkp_PRTY_TYPE_CD,
exp_insert_update_flag.lkp_BUSN_END_DT as lkp_BUSN_END_DT,
exp_insert_update_flag.lkp_ORG_TYPE_CD as lkp_ORG_TYPE_CD4,
exp_insert_update_flag.lkp_BUSN_STRT_DT as lkp_BUSN_STRT_DT,
exp_insert_update_flag.RetireFlag as RetireFlag,
exp_insert_update_flag.RETIRED as RETIRED,
exp_insert_update_flag.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_insert_update_flag.InsUpdaFlag as InsUpdaFlag,
exp_insert_update_flag.updatetime as updatetime,
exp_insert_update_flag.source_record_id
FROM
exp_insert_update_flag
WHERE exp_insert_update_flag.InsUpdaFlag = ''R'' and exp_insert_update_flag.RETIRED != 0 and exp_insert_update_flag.lkp_EDW_END_DTTM = TO_TIMESTAMP ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ));


-- Component upd_busn_retired, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_busn_retired AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_insert_update_flag_retire.lkp_BUSN_CTGY_CD as lkp_BUSN_CTGY_CD3,
rtr_insert_update_flag_retire.lkp_NK_BUSN_CD as lkp_NK_BUSN_CD3,
rtr_insert_update_flag_retire.lkp_SYS_SRC_CD as lkp_SYS_SRC_CD3,
rtr_insert_update_flag_retire.lkp_EDW_STRT_DT as lkp_EDW_STRT_DT3,
rtr_insert_update_flag_retire.lkp_PRTY_TYPE_CD as lkp_PRTY_TYPE_CD3,
rtr_insert_update_flag_retire.lkp_BUSN_END_DT as lkp_BUSN_END_DT3,
rtr_insert_update_flag_retire.lkp_ORG_TYPE_CD4 as lkp_ORG_TYPE_CD43,
rtr_insert_update_flag_retire.lkp_BUSN_STRT_DT as lkp_BUSN_STRT_DT3,
rtr_insert_update_flag_retire.updatetime as updatetime4,
1 as UPDATE_STRATEGY_ACTION,
rtr_insert_update_flag_retire.source_record_id
FROM
rtr_insert_update_flag_retire
);


-- Component upd_busn_insert, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_busn_insert AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_insert_update_flag_insert.in_BUSN_PRTY_ID as in_BUSN_PRTY_ID1,
rtr_insert_update_flag_insert.in_BUSN_STRT_DT as in_BUSN_STRT_DT1,
rtr_insert_update_flag_insert.in_BUSN_CTGY_CD as in_BUSN_CTGY_CD1,
rtr_insert_update_flag_insert.in_TAX_BRAKT_CD as in_TAX_BRAKT_CD1,
rtr_insert_update_flag_insert.in_ORG_TYPE_CD as in_ORG_TYPE_CD1,
rtr_insert_update_flag_insert.in_GICS_SBIDSTRY_CD as in_GICS_SBIDSTRY_CD1,
rtr_insert_update_flag_insert.in_BUSN_END_DT as in_BUSN_END_DT1,
rtr_insert_update_flag_insert.in_LIFCYCL_CD as in_LIFCYCL_CD1,
rtr_insert_update_flag_insert.in_PRTY_TYPE_CD as in_PRTY_TYPE_CD1,
rtr_insert_update_flag_insert.in_NK_BUSN_CD as in_NK_BUSN_CD1,
rtr_insert_update_flag_insert.in_INC_IND as in_INC_IND1,
rtr_insert_update_flag_insert.in_PRCS_ID as in_PRCS_ID1,
rtr_insert_update_flag_insert.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM1,
rtr_insert_update_flag_insert.in_EDW_END_DTTM as in_EDW_END_DTTM1,
rtr_insert_update_flag_insert.in_SYS_SRC_CD as in_SYS_SRC_CD1,
rtr_insert_update_flag_insert.in_TAX_ID_STS_CD as in_TAX_ID_STS_CD1,
rtr_insert_update_flag_insert.RETIRED as RETIRED1,
rtr_insert_update_flag_insert.updatetime as updatetime1,
0 as UPDATE_STRATEGY_ACTION,
rtr_insert_update_flag_insert.source_record_id
FROM
rtr_insert_update_flag_insert
);


-- Component exp_pass_to_target_retired, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_target_retired AS
(
SELECT
upd_busn_retired.lkp_BUSN_CTGY_CD3 as lkp_BUSN_CTGY_CD3,
upd_busn_retired.lkp_NK_BUSN_CD3 as lkp_NK_BUSN_CD3,
upd_busn_retired.lkp_SYS_SRC_CD3 as lkp_SYS_SRC_CD3,
upd_busn_retired.lkp_EDW_STRT_DT3 as lkp_EDW_STRT_DT3,
CURRENT_TIMESTAMP as out_EDW_STRT_DTTM,
upd_busn_retired.lkp_PRTY_TYPE_CD3 as lkp_PRTY_TYPE_CD3,
upd_busn_retired.lkp_BUSN_END_DT3 as lkp_BUSN_END_DT3,
upd_busn_retired.lkp_ORG_TYPE_CD43 as lkp_ORG_TYPE_CD43,
upd_busn_retired.lkp_BUSN_STRT_DT3 as lkp_BUSN_STRT_DT3,
upd_busn_retired.updatetime4 as updatetime4,
upd_busn_retired.source_record_id
FROM
upd_busn_retired
);


-- Component tgt_busn_retire, Type TARGET 
MERGE INTO DB_T_PROD_CORE.BUSN
USING exp_pass_to_target_retired ON (BUSN.BUSN_CTGY_CD = exp_pass_to_target_retired.lkp_BUSN_CTGY_CD3 AND BUSN.NK_BUSN_CD = exp_pass_to_target_retired.lkp_NK_BUSN_CD3 AND BUSN.SRC_SYS_CD = exp_pass_to_target_retired.lkp_SYS_SRC_CD3 AND BUSN.EDW_STRT_DTTM = exp_pass_to_target_retired.lkp_EDW_STRT_DT3)
WHEN MATCHED THEN UPDATE
SET
BUSN_STRT_DTTM = exp_pass_to_target_retired.lkp_BUSN_STRT_DT3,
BUSN_CTGY_CD = exp_pass_to_target_retired.lkp_BUSN_CTGY_CD3,
ORG_TYPE_CD = exp_pass_to_target_retired.lkp_ORG_TYPE_CD43,
BUSN_END_DTTM = exp_pass_to_target_retired.lkp_BUSN_END_DT3,
PRTY_TYPE_CD = exp_pass_to_target_retired.lkp_PRTY_TYPE_CD3,
NK_BUSN_CD = exp_pass_to_target_retired.lkp_NK_BUSN_CD3,
SRC_SYS_CD = exp_pass_to_target_retired.lkp_SYS_SRC_CD3,
EDW_STRT_DTTM = exp_pass_to_target_retired.lkp_EDW_STRT_DT3,
EDW_END_DTTM = exp_pass_to_target_retired.out_EDW_STRT_DTTM,
TRANS_END_DTTM = exp_pass_to_target_retired.updatetime4;


-- Component tgt_busn_retire, Type Post SQL 
UPDATE  DB_T_PROD_CORE.BUSN  FROM

(SELECT	distinct  BUSN_PRTY_ID,EDW_STRT_DTTM,TRANS_STRT_DTTM, max(EDW_STRT_DTTM) over (partition by  BUSN_PRTY_ID ORDER BY EDW_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND'' 

 as lead1

 ,max(TRANS_STRT_DTTM) over (partition by  BUSN_PRTY_ID ORDER BY TRANS_STRT_DTTM ASC rows between 1 following and 1 following)  - INTERVAL ''1 SECOND''  as lead

FROM	DB_T_PROD_CORE.BUSN

 ) a

set EDW_END_DTTM=A.lead1

, TRANS_END_DTTM=a.lead

where  BUSN.EDW_STRT_DTTM = A.EDW_STRT_DTTM

and BUSN.TRANS_STRT_DTTM = A.TRANS_STRT_DTTM

and BUSN.BUSN_PRTY_ID=A.BUSN_PRTY_ID 

and CAST(BUSN.EDW_END_DTTM AS DATE)=''9999-12-31''

and CAST(BUSN.TRANS_END_DTTM AS DATE)=''9999-12-31'' 

and lead1 is not null and lead is not null;


-- Component exp_pass_to_target_insert, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_target_insert AS
(
SELECT
upd_busn_insert.in_BUSN_PRTY_ID1 as in_BUSN_PRTY_ID1,
upd_busn_insert.in_BUSN_STRT_DT1 as in_BUSN_STRT_DT1,
upd_busn_insert.in_BUSN_CTGY_CD1 as in_BUSN_CTGY_CD1,
upd_busn_insert.in_TAX_BRAKT_CD1 as in_TAX_BRAKT_CD1,
upd_busn_insert.in_ORG_TYPE_CD1 as in_ORG_TYPE_CD1,
upd_busn_insert.in_GICS_SBIDSTRY_CD1 as in_GICS_SBIDSTRY_CD1,
upd_busn_insert.in_BUSN_END_DT1 as in_BUSN_END_DT1,
upd_busn_insert.in_LIFCYCL_CD1 as in_LIFCYCL_CD1,
upd_busn_insert.in_PRTY_TYPE_CD1 as in_PRTY_TYPE_CD1,
upd_busn_insert.in_NK_BUSN_CD1 as in_NK_BUSN_CD1,
upd_busn_insert.in_INC_IND1 as in_INC_IND1,
upd_busn_insert.in_PRCS_ID1 as in_PRCS_ID1,
upd_busn_insert.in_EDW_STRT_DTTM1 as in_EDW_STRT_DTTM1,
CASE WHEN upd_busn_insert.RETIRED1 = 0 THEN upd_busn_insert.in_EDW_END_DTTM1 ELSE upd_busn_insert.in_EDW_STRT_DTTM1 END as EDW_END_DTTM,
upd_busn_insert.in_SYS_SRC_CD1 as in_SYS_SRC_CD1,
upd_busn_insert.in_TAX_ID_STS_CD1 as in_TAX_ID_STS_CD1,
upd_busn_insert.updatetime1 as updatetime1,
CASE WHEN upd_busn_insert.RETIRED1 != 0 THEN upd_busn_insert.updatetime1 ELSE TO_TIMESTAMP ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) END as TRANS_END_DTTM,
upd_busn_insert.source_record_id
FROM
upd_busn_insert
);


-- Component tgt_busn_insert, Type TARGET 
INSERT INTO DB_T_PROD_CORE.BUSN
(
BUSN_PRTY_ID,
BUSN_STRT_DTTM,
BUSN_CTGY_CD,
TAX_BRAKT_CD,
ORG_TYPE_CD,
GICS_SBIDSTRY_CD,
BUSN_END_DTTM,
LIFCYCL_CD,
PRTY_TYPE_CD,
NK_BUSN_CD,
INC_IND,
TAX_ID_STS_CD,
SRC_SYS_CD,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM,
TRANS_END_DTTM
)
SELECT
exp_pass_to_target_insert.in_BUSN_PRTY_ID1 as BUSN_PRTY_ID,
exp_pass_to_target_insert.in_BUSN_STRT_DT1 as BUSN_STRT_DTTM,
exp_pass_to_target_insert.in_BUSN_CTGY_CD1 as BUSN_CTGY_CD,
exp_pass_to_target_insert.in_TAX_BRAKT_CD1 as TAX_BRAKT_CD,
exp_pass_to_target_insert.in_ORG_TYPE_CD1 as ORG_TYPE_CD,
exp_pass_to_target_insert.in_GICS_SBIDSTRY_CD1 as GICS_SBIDSTRY_CD,
exp_pass_to_target_insert.in_BUSN_END_DT1 as BUSN_END_DTTM,
exp_pass_to_target_insert.in_LIFCYCL_CD1 as LIFCYCL_CD,
exp_pass_to_target_insert.in_PRTY_TYPE_CD1 as PRTY_TYPE_CD,
exp_pass_to_target_insert.in_NK_BUSN_CD1 as NK_BUSN_CD,
exp_pass_to_target_insert.in_INC_IND1 as INC_IND,
exp_pass_to_target_insert.in_TAX_ID_STS_CD1 as TAX_ID_STS_CD,
COALESCE(exp_pass_to_target_insert.in_SYS_SRC_CD1, '' '') AS SRC_SYS_CD,
--exp_pass_to_target_insert.in_SYS_SRC_CD1 as SRC_SYS_CD,
exp_pass_to_target_insert.in_PRCS_ID1 as PRCS_ID,
exp_pass_to_target_insert.in_EDW_STRT_DTTM1 as EDW_STRT_DTTM,
exp_pass_to_target_insert.EDW_END_DTTM as EDW_END_DTTM,
exp_pass_to_target_insert.updatetime1 as TRANS_STRT_DTTM,
exp_pass_to_target_insert.TRANS_END_DTTM as TRANS_END_DTTM
FROM
exp_pass_to_target_insert;


END; ';