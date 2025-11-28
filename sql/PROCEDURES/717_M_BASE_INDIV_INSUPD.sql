-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_INDIV_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
 declare
	start_dttm timestamp;
	end_dttm timestamp;
    prcs_id int;
    var_prev_public_id int;
    va_Prev_prty_id int;
    var_prev_src_system char;
    run_id string;
BEGIN 
run_id :=   (SELECT run_id   FROM control_run_id where upper(worklet_name) = upper(:worklet_name) order by insert_ts desc limit 1);   
END_DTTM:=   (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''END_DTTM'' order by insert_ts desc limit 1);
START_DTTM:=     (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''START_DTTM'' order by insert_ts desc limit 1);
PRCS_ID:=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PRCS_ID'' order by insert_ts desc limit 1);
var_prev_public_id:=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PREV_PUBLIC_ID'' order by insert_ts desc limit 1);
va_Prev_prty_id:=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PREV_PRTY_ID'' order by insert_ts desc limit 1);
var_prev_src_system:=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PREV_SRC_SYSTEM'' order by insert_ts desc limit 1);

--set var_prev_public_id=1;
--set va_Prev_prty_id=1;
--set var_prev_src_system=''s'';



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


-- Component LKP_TERADATA_ETL_REF_XLAT, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT AS
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


-- Component LKP_TERADATA_ETL_REF_XLAT_CNT_NAME, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_CNT_NAME AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''BUSN_CTGY'' 

	AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM IN (''cctl_contact.typecode'',''cctl_contact.name'',''abtl_abcontact.name'')

	AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS IN (''GW'')

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


-- Component LKP_XREF_PRTY_INDIV_CLM_CTR, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_XREF_PRTY_INDIV_CLM_CTR AS
(
SELECT 
	DIR_PRTY.PRTY_ID as PRTY_ID, 
	DIR_PRTY.NK_PUBLC_ID as NK_PUBLC_ID 
FROM 
	DB_T_PROD_CORE.DIR_PRTY
WHERE
	DIR_PRTY_VAL = ''INDIV''
	AND INDIV_SRC_VAL = ''ClaimCenter''
);


-- Component LKP_XREF_PRTY_INDIV_CNT_MGR, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_XREF_PRTY_INDIV_CNT_MGR AS
(
SELECT 
	DIR_PRTY.PRTY_ID as PRTY_ID, 
	DIR_PRTY.NK_LNK_ID as NK_LNK_ID 
FROM 
	DB_T_PROD_CORE.DIR_PRTY
WHERE
	DIR_PRTY_VAL = ''INDIV''
	AND INDIV_SRC_VAL = ''ContactManager''
);


-- PIPELINE START FOR 1

-- Component sq_ab_abcontact, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_ab_abcontact AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as LinkID,
$2 as DateOfBirth,
$3 as GNDR_TYPE_CD,
$4 as TAX_FILG_TYPE_CD,
$5 as PublicID,
$6 as Source,
$7 as TAX_ID_STS_CD,
$8 as SRC_SYS_CD,
$9 as SRC_STRT_DT,
$10 as SRC_END_DT,
$11 as Retired,
$12 as Trans_Strt_dttm,
$13 as TL_CNT_Name,
$14 as Rnk,
$15 as edw_strt_dttm,
$16 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT DISTINCT

                UPPER(TMP.LinkID) AS LinkID,

                TMP.DateOfBirth AS DateOfBirth,

                TMP.GNDR_TYPE_CD,

                TMP.TAX_FILG_TYPE_CD,

                TMP.PublicID,

                TMP.SOURCE AS SOURCE,

				TMP.TAX_ID_STS_CD,

                TMP.SYS_SRC_CD, 

                case when TMP.createtime is null then TO_DATE(''19000101'',''YYYYMMDD'')   else TMP.createtime end as SRC_STRT_DT,

                to_TIMESTAMP (''9999-12-31 23:59:59.999999'') AS SRC_END_DT,

                TMP.retired,

      TMP.updatetime,

      TMP.TL_CNT_Name,

      ROW_NUMBER() OVER(PARTITION BY case when source =''ClaimCenter'' then PublicID else LinkID end,source ORDER BY updatetime, LinkID,DateOfBirth) as RNK,

	  current_timestamp edw_strt_dttm 	  from (SELECT * 

FROM

/* DB_T_PROD_STAG.ab_abcontact */
(

SELECT 

	

	bc_contact.UpdateTime_stg AS updatetime , 

 	cast(bc_contact.PublicID_stg as varchar(64)) AS PublicID, 

	cast(bc_contact.AddressBookUID_stg as varchar(64)) AS LinkID, 

	bc_contact.Retired_stg AS retired, 

	bc_contact.CreateTime_stg AS CreateTime, 

	bc_contact.DateOfBirth_stg AS DateOfBirth, 

/* bctl_gendertype.TYPECODE_stg AS GNDR_TYPE_CD,  */
	CAST(NULL AS VARCHAR(50)) AS GNDR_TYPE_CD, 

	bctl_taxfilingstatustype.TYPECODE_stg AS TAX_FILG_TYPE_CD,  

	bctl_contact.NAME_stg AS TL_CNT_Name, 

	bctl_taxstatus.typecode_stg AS TAX_ID_STS_CD, 

	''SRC_SYS5'' AS SYS_SRC_CD,

	cast(''ClaimCenter'' as varchar(50)) AS SOURCE

	

	

FROM

	DB_T_PROD_STAG.bc_contact 

	LEFT OUTER JOIN DB_T_PROD_STAG.bctl_contact ON bctl_contact.id_stg = bc_contact.subtype_stg 

	LEFT OUTER JOIN DB_T_PROD_STAG.bctl_gendertype ON bc_contact.gender_stg = bctl_gendertype.id_stg 

	LEFT OUTER JOIN DB_T_PROD_STAG.bctl_taxfilingstatustype ON bc_contact.TaxFilingStatus_stg = bctl_taxfilingstatustype.id_stg 

	LEFT OUTER JOIN DB_T_PROD_STAG.bctl_taxstatus ON bc_contact.taxstatus_stg = bctl_taxstatus.id_stg

	LEFT OUTER JOIN DB_T_PROD_STAG.bctl_maritalstatus ON bc_contact.Maritalstatus_stg = bctl_maritalstatus.id_stg 

	LEFT OUTER JOIN DB_T_PROD_STAG.bctl_nameprefix ON bc_contact.prefix_stg = bctl_nameprefix.id_stg 

	LEFT OUTER JOIN DB_T_PROD_STAG.bctl_namesuffix ON bc_contact.Suffix_stg = bctl_namesuffix.id_stg

	LEFT OUTER JOIN DB_T_PROD_STAG.bc_user ON bc_user.ContactID_stg = bc_contact.id_stg

	LEFT OUTER JOIN DB_T_PROD_STAG.bc_credential on bc_user.CredentialID_stg = bc_credential.id_stg

WHERE

	bctl_contact.typecode_stg = (''UserContact'')  AND 

/*  below condition added to avoid duplicates */
       bc_contact.publicid_stg not in (''default_data:1'', ''systemTables:1'',''systemTables:2'') 

	and ((bc_contact.UpdateTime_stg>( :START_DTTM) AND bc_contact.UpdateTime_stg <=( :end_dttm)) OR

	(bc_user.UpdateTime_stg>( :START_DTTM) AND bc_user.UpdateTime_stg <= ( :end_dttm)))



UNION



/*  Primary and Secondary Payer contact (this is at the Account level) */


SELECT 



    case when bc_contact.UpdateTime_stg > a.UpdateTime_stg then bc_contact.UpdateTime_stg else a.UpdateTime_stg end  AS updatetime,

 	case when(bc_contact.ExternalID_stg is not null) then bc_contact.ExternalID_stg else bc_contact.PublicID_stg end AS PublicID, 

	cast(bc_contact.AddressBookUID_stg as varchar(64)) AS LinkID, 

	bc_contact.Retired_stg AS retired, 

	bc_contact.CreateTime_stg AS CreateTime, 

	bc_contact.DateOfBirth_stg AS DateOfBirth, 

/* bctl_gendertype.TYPECODE_stg AS GNDR_TYPE_CD,  */
	CAST(NULL AS VARCHAR(50)) AS GNDR_TYPE_CD,

	bctl_taxfilingstatustype.TYPECODE_stg AS TAX_FILG_TYPE_CD, 

	bctl_contact.NAME_stg AS TL_CNT_Name, 

	bctl_taxstatus.typecode_stg AS TAX_ID_STS_CD, 

	''SRC_SYS5'' AS SYS_SRC_CD,

	cast(''ClaimCenter'' as varchar(50)) AS SOURCE



from DB_T_PROD_STAG.bc_account a

inner join DB_T_PROD_STAG.bc_accountcontact h on h.AccountID_stg = a.id_stg

inner join DB_T_PROD_STAG.bc_contact on bc_contact.id_stg = h.ContactID_stg

join DB_T_PROD_STAG.bctl_contact on bctl_contact.ID_stg=bc_contact.Subtype_stg

left join DB_T_PROD_STAG.bc_accountcontactrole i on i.AccountContactID_stg = h.id_stg

left join DB_T_PROD_STAG.bctl_accountrole j on j.id_stg = i.Role_stg

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_gendertype ON bc_contact.gender_stg = bctl_gendertype.id_stg 

	LEFT OUTER JOIN DB_T_PROD_STAG.bctl_taxfilingstatustype ON bc_contact.TaxFilingStatus_stg = bctl_taxfilingstatustype.id_stg 

	LEFT OUTER JOIN DB_T_PROD_STAG.bctl_taxstatus ON bc_contact.taxstatus_stg = bctl_taxstatus.id_stg

	LEFT OUTER JOIN DB_T_PROD_STAG.bctl_maritalstatus ON bc_contact.Maritalstatus_stg = bctl_maritalstatus.id_stg 

	LEFT OUTER JOIN DB_T_PROD_STAG.bctl_nameprefix ON bc_contact.prefix_stg = bctl_nameprefix.id_stg 

	LEFT OUTER JOIN DB_T_PROD_STAG.bctl_namesuffix ON bc_contact.Suffix_stg = bctl_namesuffix.id_stg

	LEFT OUTER JOIN DB_T_PROD_STAG.bc_user ON bc_user.ContactID_stg = bc_contact.id_stg

	LEFT OUTER JOIN DB_T_PROD_STAG.bc_credential on bc_user.CredentialID_stg = bc_credential.id_stg

where ((h.PrimaryPayer_stg = 1) or (j.name_stg = ''Payer''))

and ((bc_contact.updatetime_stg>( :START_DTTM) 

        and bc_contact.updatetime_stg <=( :end_dttm)) 

        or (bc_user.updatetime_stg>( :START_DTTM)   

        and bc_user.updatetime_stg <= ( :end_dttm))

        or (a.updatetime_stg>( :START_DTTM)     

        and a.updatetime_stg <= ( :end_dttm)))

UNION

/*  Primary Payer and Overiding Payer Contact (this is at the Invoicestream level) */


SELECT 

	

    case when bc_contact.UpdateTime_stg > a.UpdateTime_stg then bc_contact.UpdateTime_stg else a.UpdateTime_stg end  AS updatetime,

 	case when (bc_contact.ExternalID_stg is null) then bc_contact.PublicID_stg else bc_contact.ExternalID_stg end AS PublicID, 

	cast(bc_contact.AddressBookUID_stg as varchar(64)) AS LinkID, 

	bc_contact.Retired_stg AS retired, 

	bc_contact.CreateTime_stg AS CreateTime, 

	bc_contact.DateOfBirth_stg AS DateOfBirth, 

/* bctl_gendertype.TYPECODE_stg AS GNDR_TYPE_CD, */
	CAST(NULL AS VARCHAR(50)) AS GNDR_TYPE_CD,

	bctl_taxfilingstatustype.TYPECODE_stg AS TAX_FILG_TYPE_CD,

	bctl_contact.NAME_stg AS TL_CNT_Name, 

	bctl_taxstatus.typecode_stg AS TAX_ID_STS_CD, 

	''SRC_SYS5'' AS SYS_SRC_CD,

	cast(''ClaimCenter'' as varchar(50)) AS SOURCE



from DB_T_PROD_STAG.bc_account a

inner join DB_T_PROD_STAG.bc_invoicestream b on a.id_stg = b.AccountID_stg

inner join DB_T_PROD_STAG.bc_accountcontact c on c.accountid_stg=a.id_stg

inner join DB_T_PROD_STAG.bc_contact on bc_contact.id_stg = c.ContactID_stg

join DB_T_PROD_STAG.bctl_contact on bctl_contact.ID_stg=bc_contact.Subtype_stg

left join DB_T_PROD_STAG.bc_accountcontactrole f on f.AccountContactID_stg = c.id_stg

left join DB_T_PROD_STAG.bctl_accountrole g on g.id_stg = f.Role_stg

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_gendertype ON bc_contact.gender_stg = bctl_gendertype.id_stg 

	LEFT OUTER JOIN DB_T_PROD_STAG.bctl_taxfilingstatustype ON bc_contact.TaxFilingStatus_stg = bctl_taxfilingstatustype.id_stg 

	LEFT OUTER JOIN DB_T_PROD_STAG.bctl_taxstatus ON bc_contact.taxstatus_stg = bctl_taxstatus.id_stg

	LEFT OUTER JOIN DB_T_PROD_STAG.bctl_maritalstatus ON bc_contact.Maritalstatus_stg = bctl_maritalstatus.id_stg 

	LEFT OUTER JOIN DB_T_PROD_STAG.bctl_nameprefix ON bc_contact.prefix_stg = bctl_nameprefix.id_stg 

	LEFT OUTER JOIN DB_T_PROD_STAG.bctl_namesuffix ON bc_contact.Suffix_stg = bctl_namesuffix.id_stg

	LEFT OUTER JOIN DB_T_PROD_STAG.bc_user ON bc_user.ContactID_stg = bc_contact.id_stg

	LEFT OUTER JOIN DB_T_PROD_STAG.bc_credential on bc_user.CredentialID_stg = bc_credential.id_stg

where ((b.OverridingPayer_alfa_stg is null and c.PrimaryPayer_stg = 1) or (b.OverridingPayer_alfa_stg is not null))

and ((bc_contact.updatetime_stg>( :START_DTTM) 

        and bc_contact.updatetime_stg <=( :end_dttm)) 

        or (bc_user.updatetime_stg>( :START_DTTM)   

        and bc_user.updatetime_stg <= ( :end_dttm))

        or (a.updatetime_stg>( :START_DTTM)     

        and a.updatetime_stg <= ( :end_dttm)))

UNION



SELECT 

	 

	pc_contact.UpdateTime_stg AS updatetime,

	cast(pc_contact.PublicID_stg as varchar(64)) AS PublicID, 

	cast(pc_contact.AddressBookUID_stg as varchar(64)) AS LinkID, 

	pc_contact.Retired_stg AS retired, 

	pc_contact.CreateTime_stg AS CreateTime, 

	pc_contact.DateOfBirth_stg AS DateOfBirth, 

	pctl_gendertype.TYPECODE_stg AS GNDR_TYPE_CD,

	pctl_taxfilingstatustype.TYPECODE_stg AS TAX_FILG_TYPE_CD,

	pctl_contact.NAME_stg AS TL_CNT_Name,

	pctl_taxstatus.typecode_stg AS TAX_ID_STS_CD,

	''SRC_SYS4'' AS SYS_SRC_CD,

	cast(''ClaimCenter'' as varchar(50)) AS SOURCE





FROM

	DB_T_PROD_STAG.pc_contact 

	LEFT OUTER JOIN DB_T_PROD_STAG.pctl_contact ON pctl_contact.id_stg = pc_contact.subtype_stg 

	LEFT OUTER JOIN DB_T_PROD_STAG.pctl_gendertype ON pc_contact.gender_stg = pctl_gendertype.id_stg 

	LEFT OUTER JOIN DB_T_PROD_STAG.pctl_taxfilingstatustype ON pc_contact.TaxFilingStatus_stg = pctl_taxfilingstatustype.id_stg 

	LEFT OUTER JOIN DB_T_PROD_STAG.pctl_taxstatus ON pc_contact.taxstatus_stg = pctl_taxstatus.id_stg

	LEFT OUTER JOIN DB_T_PROD_STAG.pctl_maritalstatus ON pc_contact.Maritalstatus_stg = pctl_maritalstatus.id_stg 

	LEFT OUTER JOIN DB_T_PROD_STAG.pctl_nameprefix ON pc_contact.prefix_stg = pctl_nameprefix.id_stg 

	LEFT OUTER JOIN DB_T_PROD_STAG.pctl_namesuffix ON pc_contact.Suffix_stg = pctl_namesuffix.id_stg

	LEFT OUTER JOIN DB_T_PROD_STAG.pc_user ON pc_user.ContactID_stg = pc_contact.id_stg

	LEFT OUTER JOIN DB_T_PROD_STAG.pc_credential on pc_user.CredentialID_stg = pc_credential.id_stg

LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod on pc_policyperiod.PNIContactDenorm_stg = pc_contact.ID_stg

LEFT OUTER JOIN DB_T_PROD_STAG.pc_effectivedatedfields on pc_effectivedatedfields.BranchID_stg=pc_policyperiod.ID_stg

LEFT OUTER JOIN DB_T_PROD_STAG.pc_producercode on pc_producercode.ID_stg=pc_effectivedatedfields.ProducerCodeID_stg

WHERE

	pctl_contact.typecode_stg = (''UserContact'')  AND 

/*  below condition added to avoid duplicates */
       pc_contact.publicid_stg not in (''default_data:1'', ''systemTables:1'',''systemTables:2'') 

	AND ((pc_contact.UpdateTime_stg>( :START_DTTM) AND pc_contact.UpdateTime_stg <= ( :end_dttm)) OR 

	 (pc_user.UpdateTime_stg>( :START_DTTM) AND pc_user.UpdateTime_stg <= ( :end_dttm)))



UNION



SELECT DISTINCT 

	

	cc_contact.UpdateTime_stg AS updatetime, 

	cast(cc_contact.PublicID_stg as varchar(64)) AS PublicID, 

	cast(cc_contact.AddressBookUID_stg as varchar(64)) AS LinkID, 

	cc_contact.Retired_stg AS retired, 

	cc_contact.CreateTime_stg AS CreateTime, 

	cc_contact.DateOfBirth_stg AS DateOfBirth, 

	cctl_gendertype.TYPECODE_stg AS GNDR_TYPE_CD,

	cctl_taxfilingstatustype.TYPECODE_stg AS TAX_FILG_TYPE_CD,

	cctl_contact.NAME_stg AS TL_CNT_Name,

	cctl_taxstatus.typecode_stg AS TAX_ID_STS_CD,

	''SRC_SYS6'' AS SYS_SRC_CD,

	cast(''ClaimCenter'' as varchar(50)) AS SOURCE



FROM

	DB_T_PROD_STAG.cc_contact 

	LEFT OUTER JOIN DB_T_PROD_STAG.cctl_contact ON cctl_contact.id_stg = cc_contact.subtype_stg 

	LEFT OUTER JOIN DB_T_PROD_STAG.cctl_gendertype ON cc_contact.gender_stg = cctl_gendertype.id_stg 

	LEFT OUTER JOIN DB_T_PROD_STAG.cctl_taxfilingstatustype ON cc_contact.TaxFilingStatus_stg = cctl_taxfilingstatustype.id_stg 

	LEFT OUTER JOIN DB_T_PROD_STAG.cctl_taxstatus ON cc_contact.taxstatus_stg = cctl_taxstatus.id_stg

	LEFT OUTER JOIN DB_T_PROD_STAG.cctl_maritalstatus ON cc_contact.Maritalstatus_stg = cctl_maritalstatus.id_stg 

	LEFT OUTER JOIN DB_T_PROD_STAG.cctl_nameprefix ON cc_contact.prefix_stg = cctl_nameprefix.id_stg 

	LEFT OUTER JOIN DB_T_PROD_STAG.cctl_namesuffix ON cc_contact.Suffix_stg = cctl_namesuffix.id_stg

	LEFT OUTER JOIN DB_T_PROD_STAG.cc_user ON cc_user.ContactID_stg = cc_contact.id_stg

	LEFT OUTER JOIN DB_T_PROD_STAG.cc_credential ON cc_user.CredentialID_stg = cc_credential.id_stg

	LEFT OUTER JOIN DB_T_PROD_STAG.cc_claimcontact ON cc_contact.id_stg=cc_claimcontact.contactid_stg

	LEFT OUTER JOIN DB_T_PROD_STAG.cc_claimcontactrole ON cc_claimcontact.id_stg=cc_claimcontactrole.claimcontactid_stg

	LEFT OUTER JOIN DB_T_PROD_STAG.cc_incident ON cc_claimcontactrole.claimcontactid_stg =cc_incident.ID_stg

WHERE

	(cc_contact.UpdateTime_stg>( :START_DTTM) AND cc_contact.UpdateTime_stg <= ( :end_dttm) )  or 

       (cc_user.UpdateTime_stg>( :START_DTTM) AND cc_user.UpdateTime_stg <=( :end_dttm) )

	  

UNION 



SELECT 

	 

	ab_abcontact.UpdateTime_stg AS updatetime, 

	CAST(NULL AS VARCHAR(50)) AS PublicID ,

	ab_abcontact.LinkID_stg AS LinkID, 

 	ab_abcontact.Retired_stg AS retired, 

	ab_abcontact.CreateTime_stg AS CreateTime,

	ab_abcontact.DateOfBirth_stg AS DateOfBirth, 

	abtl_gendertype.TYPECODE_stg AS GNDR_TYPE_CD,

	abtl_taxfilingstatustype.TYPECODE_stg AS TAX_FILG_TYPE_CD,

	abtl_abcontact.NAME_stg AS TL_CNT_Name,

	abtl_taxstatus.typecode_stg AS TAX_ID_STS_CD,

	''SRC_SYS7'' AS SYS_SRC_CD,

	cast(''ContactManager'' as varchar(50)) as SOURCE

	

FROM

 	DB_T_PROD_STAG.ab_abcontact

	LEFT OUTER JOIN DB_T_PROD_STAG.abtl_abcontact ON abtl_abcontact.id_stg = ab_abcontact.subtype_stg 

	LEFT OUTER JOIN DB_T_PROD_STAG.abtl_gendertype ON ab_abcontact.gender_stg = abtl_gendertype.id_stg 

	LEFT OUTER JOIN DB_T_PROD_STAG.abtl_taxfilingstatustype ON ab_abcontact.TaxFilingStatus_stg = abtl_taxfilingstatustype.id_stg 

	LEFT OUTER JOIN DB_T_PROD_STAG.abtl_taxstatus ON ab_abcontact.taxstatus_stg = abtl_taxstatus.id_stg

	LEFT OUTER JOIN DB_T_PROD_STAG.abtl_maritalstatus ON ab_abcontact.Maritalstatus_stg = abtl_maritalstatus.id_stg 

	LEFT OUTER JOIN DB_T_PROD_STAG.abtl_nameprefix ON ab_abcontact.prefix_stg = abtl_nameprefix.id_stg 

	LEFT OUTER JOIN DB_T_PROD_STAG.abtl_namesuffix ON ab_abcontact.Suffix_stg = abtl_namesuffix.id_stg

	LEFT OUTER JOIN DB_T_PROD_STAG.ab_user ON ab_user.ContactID_stg = ab_abcontact.id_stg

	LEFT OUTER JOIN DB_T_PROD_STAG.ab_credential on ab_user.CredentialID_stg = ab_credential.id_stg

	LEFT OUTER JOIN DB_T_PROD_STAG.abtl_occupation ON  abtl_occupation.ID_stg = ab_abcontact.occupation_alfa_stg

WHERE

(ab_abcontact.UpdateTime_stg>( :START_DTTM) AND ab_abcontact.UpdateTime_stg <= ( :end_dttm))



) TMP



WHERE  

                TMP.TL_CNT_Name IN (''Person'',''Adjudicator'',''User Contact'',''Vendor (Person)'',''Attorney'', ''Doctor'',''Policy Person'',''Contact'',''Lodging (Person)'') 

				AND ((TMP.SOURCE = ''ClaimCenter'' AND TMP.PublicID IS NOT NULL) OR (TMP.SOURCE = ''ContactManager'' AND TMP.LinkID IS NOT NULL))

				

				QUALIFY ROW_NUMBER() OVER(PARTITION BY case when source =''ClaimCenter'' then PublicID else LinkID end,tmp.source,SYS_SRC_CD/*,DateOfBirth,TMP.GNDR_TYPE_CD,TMP.TAX_FILG_TYPE_CD,TMP.PublicID,TMP.SOURCE,TMP.TAX_ID_STS_CD,TMP.SYS_SRC_CD,SRC_STRT_DT,SRC_END_DT,TMP.Retired */

			   ORDER BY TMP.updatetime DESC,DateOfBirth desc,tmp.createtime desc )=1

			   )tmp 			  ORDER BY PublicID,LinkID ,updatetime
) SRC
)
);


-- Component exp_all_source, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_all_source AS
(
SELECT
sq_ab_abcontact.LinkID as in_LinkID,
CASE WHEN sq_ab_abcontact.DateOfBirth IS NULL THEN to_Date ( ''1900-01-01'' , ''yyyy-mm-dd'' ) ELSE sq_ab_abcontact.DateOfBirth END as DateOfBirth1,
sq_ab_abcontact.GNDR_TYPE_CD as GNDR_TYPE_CD,
sq_ab_abcontact.TAX_FILG_TYPE_CD as TAX_FILG_TYPE_CD,
DECODE ( sq_ab_abcontact.Source , ''ClaimCenter'' , sq_ab_abcontact.PublicID , ''ContactManager'' , sq_ab_abcontact.LinkID ) as v_PublicID,
ltrim ( rtrim ( v_PublicID ) ) as out_PublicIDlkp,
sq_ab_abcontact.PublicID as o_PublicID,
sq_ab_abcontact.Source as Source,
DECODE ( TRUE , LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT */ IS NULL , ''UNK'' , LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT */ ) as out_TAX_ID_STS_CD,
:PRCS_ID as PROCESS_ID,
DECODE ( TRUE , sq_ab_abcontact.Source = ''ContactManager'' , LKP_3.INDIV_PRTY_ID /* replaced lookup LKP_INDIV_CNT_MGR */ ) as var_cnt_mgr_INDIV_PRTY_ID,
DECODE ( TRUE , sq_ab_abcontact.Source = ''ClaimCenter'' , LKP_4.INDIV_PRTY_ID /* replaced lookup LKP_INDIV_CLM_CTR */ ) as var_clm_ctr_INDIV_PRTY_ID,
DECODE ( TRUE , sq_ab_abcontact.Source = ''ContactManager'' , var_cnt_mgr_INDIV_PRTY_ID , sq_ab_abcontact.Source = ''ClaimCenter'' , var_clm_ctr_INDIV_PRTY_ID , 9999 ) as INDIV_PRTY_ID,
''UNK'' as TAX_BRAKT_CD,
''UNK'' as NTLTY_CD,
''UNK'' as LIFCYL_CD,
''UNK'' as PRTY_TYPE_CD,
ltrim ( rtrim ( LKP_5.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SRC_CD */ ) ) as SRC_SYS_CD,
sq_ab_abcontact.SRC_STRT_DT as SRC_STRT_DT,
sq_ab_abcontact.SRC_END_DT as SRC_END_DT,
CURRENT_TIMESTAMP as in_EDW_STRT_DTTM,
TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as in_EDW_END_DTTM,
sq_ab_abcontact.Retired as Retired,
ltrim ( rtrim ( LKP_6.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_CNT_NAME */ ) ) as out_TL_CNT_Name,
sq_ab_abcontact.Rnk as Rnk,
sq_ab_abcontact.edw_strt_dttm as edw_strt_dttm,
sq_ab_abcontact.source_record_id,
row_number() over (partition by sq_ab_abcontact.source_record_id order by sq_ab_abcontact.source_record_id) as RNK1
FROM
sq_ab_abcontact
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = sq_ab_abcontact.TAX_ID_STS_CD
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = sq_ab_abcontact.TAX_ID_STS_CD
LEFT JOIN LKP_INDIV_CNT_MGR LKP_3 ON LKP_3.NK_LINK_ID = sq_ab_abcontact.LinkID
LEFT JOIN LKP_INDIV_CLM_CTR LKP_4 ON LKP_4.NK_PUBLC_ID = sq_ab_abcontact.PublicID
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_SRC_CD LKP_5 ON LKP_5.SRC_IDNTFTN_VAL = sq_ab_abcontact.SRC_SYS_CD
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_CNT_NAME LKP_6 ON LKP_6.SRC_IDNTFTN_VAL = sq_ab_abcontact.TL_CNT_Name
QUALIFY RNK1 = 1
);


-- Component LKP_INDIV, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_INDIV AS
(
SELECT
LKP.INDIV_PRTY_ID,
LKP.INDIV_STRT_DTTM,
LKP.BIRTH_DT,
LKP.GNDR_TYPE_CD,
LKP.TAX_BRAKT_CD,
LKP.NTLTY_CD,
LKP.INDIV_END_DTTM,
LKP.LIFCYCL_CD,
LKP.PRTY_TYPE_CD,
LKP.TAX_FILG_TYPE_CD,
LKP.NK_LINK_ID,
LKP.NK_PUBLC_ID1,
LKP.TAX_ID_STS_CD,
LKP.EDW_STRT_DTTM,
LKP.EDW_END_DTTM,
LKP.SRC_SYS_CD,
LKP.TRANS_STRT_DTTM,
LKP.INDIV_CTGY_CD,
exp_all_source.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_all_source.source_record_id ORDER BY LKP.INDIV_PRTY_ID asc,LKP.INDIV_STRT_DTTM asc,LKP.BIRTH_DT asc,LKP.DEATH_DT asc,LKP.GNDR_TYPE_CD asc,LKP.MM_OBJT_ID asc,LKP.ETHCTY_TYPE_CD asc,LKP.TAX_BRAKT_CD asc,LKP.VIP_TYPE_CD asc,LKP.RETIRMT_DT asc,LKP.EMPLMT_STRT_DT asc,LKP.NTLTY_CD asc,LKP.PRTY_DESC asc,LKP.INDIV_END_DTTM asc,LKP.LIFCYCL_CD asc,LKP.PRTY_TYPE_CD asc,LKP.INIT_DATA_SRC_TYPE_CD asc,LKP.SSN_TAX_NUM asc,LKP.TAX_FILG_TYPE_CD asc,LKP.NK_LINK_ID asc,LKP.NK_PUBLC_ID1 asc,LKP.PRCS_ID asc,LKP.TAX_ID_STS_CD asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc,LKP.SRC_SYS_CD asc,LKP.TRANS_STRT_DTTM asc,LKP.INDIV_CTGY_CD asc) RNK
FROM
exp_all_source
LEFT JOIN (
SELECT	INDIV.INDIV_PRTY_ID as INDIV_PRTY_ID,
		INDIV.INDIV_STRT_DTTM as INDIV_STRT_DTTM, INDIV.BIRTH_DT as BIRTH_DT,
		INDIV.DEATH_DT as DEATH_DT, INDIV.GNDR_TYPE_CD as GNDR_TYPE_CD,
		INDIV.MM_OBJT_ID as MM_OBJT_ID, INDIV.ETHCTY_TYPE_CD as ETHCTY_TYPE_CD,
		INDIV.TAX_BRAKT_CD as TAX_BRAKT_CD, INDIV.VIP_TYPE_CD as VIP_TYPE_CD,
		INDIV.RETIRMT_DT as RETIRMT_DT, INDIV.EMPLMT_STRT_DT as EMPLMT_STRT_DT,
		INDIV.NTLTY_CD as NTLTY_CD, INDIV.PRTY_DESC as PRTY_DESC, INDIV.INDIV_END_DTTM as INDIV_END_DTTM,
		INDIV.LIFCYCL_CD as LIFCYCL_CD, INDIV.PRTY_TYPE_CD as PRTY_TYPE_CD,
		INDIV.INIT_DATA_SRC_TYPE_CD as INIT_DATA_SRC_TYPE_CD, INDIV.SSN_TAX_NUM as SSN_TAX_NUM,
		INDIV.TAX_FILG_TYPE_CD as TAX_FILG_TYPE_CD, INDIV.NK_LINK_ID as NK_LINK_ID,
		INDIV.PRCS_ID as PRCS_ID, INDIV.TAX_ID_STS_CD as TAX_ID_STS_CD,
		INDIV.EDW_STRT_DTTM as EDW_STRT_DTTM, INDIV.EDW_END_DTTM as EDW_END_DTTM,
		INDIV.TRANS_STRT_DTTM as TRANS_STRT_DTTM, INDIV.INDIV_CTGY_CD as INDIV_CTGY_CD,
	CASE  INDIV.SRC_SYS_CD 
			WHEN ''GWCC'' THEN ltrim(rtrim(INDIV.NK_PUBLC_ID)) 
			WHEN ''CM'' THEN ltrim(rtrim(INDIV.NK_LINK_ID)) 
			WHEN ''GWPC'' THEN ltrim(rtrim(INDIV.NK_PUBLC_ID)) 
			when ''GWBC'' THEN ltrim(rtrim(INDIV.NK_PUBLC_ID)) 
		END as NK_PUBLC_ID1, ltrim(rtrim(INDIV.SRC_SYS_CD)) as SRC_SYS_CD 
FROM	DB_T_PROD_CORE.INDIV qualify	row_number () over (
partition by NK_PUBLC_ID1, SRC_SYS_CD  
order by EDW_END_DTTM desc)=1
) LKP ON LKP.NK_PUBLC_ID1 = exp_all_source.out_PublicIDlkp AND LKP.SRC_SYS_CD = exp_all_source.SRC_SYS_CD
QUALIFY RNK = 1
);


-- Component exp_compare_data, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_compare_data AS
(
SELECT
LKP_INDIV.INDIV_PRTY_ID as lkp_INDIV_PRTY_ID,
LKP_INDIV.NK_PUBLC_ID1 as lkp_NK_PUBLC_ID,
LKP_INDIV.SRC_SYS_CD as SRC_SYS_CD,
LKP_INDIV.NK_LINK_ID as lkp_NK_LINK_ID,
LKP_INDIV.EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
LKP_INDIV.EDW_END_DTTM as lkp_EDW_END_DTTM,
LKP_INDIV.TRANS_STRT_DTTM as lkp_TRANS_STRT_DTTM1,

MD5 ( LTRIM ( RTRIM ( TO_CHAR ( nvl(LKP_INDIV.INDIV_STRT_DTTM,''1900-01-01'') , ''yyyy-mm-dd'' ) ) ) || LTRIM ( RTRIM ( TO_CHAR ( nvl(LKP_INDIV.BIRTH_DT,''1900-01-01'') , ''yyyy-mm-dd'' ) ) ) || LTRIM ( RTRIM ( UPPER ( nvl(LKP_INDIV.GNDR_TYPE_CD,'''') ) ) ) || LTRIM ( RTRIM ( nvl(LKP_INDIV.TAX_BRAKT_CD,'''') ) ) || LTRIM ( RTRIM ( nvl(LKP_INDIV.NTLTY_CD,'''') ) ) || LTRIM ( RTRIM ( nvl(LKP_INDIV.LIFCYCL_CD,'''') ) ) || LTRIM ( RTRIM ( nvl(LKP_INDIV.PRTY_TYPE_CD,'''') ) ) || LTRIM ( RTRIM ( UPPER ( nvl(LKP_INDIV.TAX_ID_STS_CD,'''') ) ) ) || LTRIM ( RTRIM ( UPPER ( nvl(LKP_INDIV.INDIV_CTGY_CD,'''') ) ) ) ) as v_lkp_checksum,
exp_all_source.SRC_STRT_DT as in_SRC_STRT_DT,
exp_all_source.DateOfBirth1 as in_DateOfBirth,
exp_all_source.GNDR_TYPE_CD as in_GNDR_TYPE_CD,
exp_all_source.TAX_BRAKT_CD as in_TAX_BRAKT_CD,
exp_all_source.NTLTY_CD as in_NTLTY_CD,
exp_all_source.LIFCYL_CD as in_LIFCYL_CD,
exp_all_source.PRTY_TYPE_CD as in_PRTY_TYPE_CD,
exp_all_source.TAX_FILG_TYPE_CD as in_TAX_FILG_TYPE_CD,
exp_all_source.Source as in_Source,
exp_all_source.in_LinkID as in_LinkID,
exp_all_source.out_TAX_ID_STS_CD as in_TAX_ID_STS_CD,
exp_all_source.o_PublicID as in_PublicID,
exp_all_source.SRC_END_DT as in_SRC_END_DT,
exp_all_source.PROCESS_ID as PROCESS_ID,
exp_all_source.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_all_source.in_EDW_END_DTTM as in_EDW_END_DTTM,
exp_all_source.out_TL_CNT_Name as in_TL_CNT_Name,
exp_all_source.SRC_SYS_CD as SRC_SYS_CD1,

MD5 ( LTRIM ( RTRIM ( TO_CHAR ( nvl(exp_all_source.SRC_STRT_DT,''1900-01-01'') , ''yyyy-mm-dd'' ) ) ) || LTRIM ( RTRIM ( TO_CHAR ( nvl(exp_all_source.DateOfBirth1,''1900-01-01'') , ''yyyy-mm-dd'' ) ) ) || LTRIM ( RTRIM ( UPPER ( nvl(exp_all_source.GNDR_TYPE_CD,'''') ) ) ) || LTRIM ( RTRIM ( nvl(exp_all_source.TAX_BRAKT_CD,'''') ) ) || LTRIM ( RTRIM ( nvl(exp_all_source.NTLTY_CD,'''') ) ) || LTRIM ( RTRIM ( nvl(exp_all_source.LIFCYL_CD,'''') ) ) || LTRIM ( RTRIM ( nvl(exp_all_source.PRTY_TYPE_CD,'''') ) ) || LTRIM ( RTRIM ( UPPER ( nvl(exp_all_source.out_TAX_ID_STS_CD,'''') ) ) ) || LTRIM ( RTRIM ( UPPER ( nvl(exp_all_source.out_TL_CNT_Name,'''') ) ) ) ) as v_in_checksum,


CASE WHEN v_lkp_checksum IS NULL THEN ''I'' ELSE CASE WHEN v_lkp_checksum != v_in_checksum THEN ''U'' ELSE ''R'' END END as calc_ins_upd,
exp_all_source.Retired as Retired,
sq_ab_abcontact.Trans_Strt_dttm as Trans_Strt_dttm,
exp_all_source.out_PublicIDlkp as out_PublicIDlkp,
exp_all_source.Rnk as Rnk,
exp_all_source.edw_strt_dttm as edw_strt_dttm,
sq_ab_abcontact.source_record_id
FROM
sq_ab_abcontact
INNER JOIN exp_all_source ON sq_ab_abcontact.source_record_id = exp_all_source.source_record_id
INNER JOIN LKP_INDIV ON exp_all_source.source_record_id = LKP_INDIV.source_record_id
);


-- Component rtr_insert_update_flag_Insert, Type ROUTER Output Group Insert
CREATE OR REPLACE TEMPORARY TABLE rtr_insert_update_flag_Insert AS
(SELECT
exp_compare_data.lkp_INDIV_PRTY_ID as lkp_INDIV_PRTY_ID,
exp_compare_data.lkp_NK_PUBLC_ID as lkp_NK_PUBLC_ID,
exp_compare_data.SRC_SYS_CD as SRC_SYS_CD,
exp_compare_data.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_compare_data.in_SRC_STRT_DT as in_SRC_STRT_DT,
exp_compare_data.in_DateOfBirth as in_DateOfBirth,
exp_compare_data.in_GNDR_TYPE_CD as in_GNDR_TYPE_CD,
exp_compare_data.in_TAX_BRAKT_CD as in_TAX_BRAKT_CD,
exp_compare_data.in_NTLTY_CD as in_NTLTY_CD,
exp_compare_data.in_LIFCYL_CD as in_LIFCYL_CD,
exp_compare_data.in_PRTY_TYPE_CD as in_PRTY_TYPE_CD,
exp_compare_data.in_TAX_FILG_TYPE_CD as in_TAX_FILG_TYPE_CD,
exp_compare_data.in_LinkID as in_LinkID,
exp_compare_data.in_TAX_ID_STS_CD as in_TAX_ID_STS_CD,
exp_compare_data.in_PublicID as in_PublicID,
exp_compare_data.in_SRC_END_DT as in_SRC_END_DT,
exp_compare_data.PROCESS_ID as PROCESS_ID,
exp_compare_data.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_compare_data.in_EDW_END_DTTM as in_EDW_END_DTTM,
exp_compare_data.SRC_SYS_CD1 as SRC_SYS_CD4,
exp_compare_data.calc_ins_upd as calc_ins_upd,
exp_compare_data.lkp_NK_LINK_ID as lkp_NK_LINK_ID,
exp_compare_data.in_Source as in_Source,
exp_compare_data.Retired as Retired,
exp_compare_data.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_compare_data.Trans_Strt_dttm as Trans_Strt_dttm,
exp_compare_data.lkp_TRANS_STRT_DTTM1 as lkp_TRANS_STRT_DTTM1,
exp_compare_data.out_PublicIDlkp as out_PublicIDlkp,
exp_compare_data.in_TL_CNT_Name as TL_CNT_Name,
exp_compare_data.Rnk as Rnk,
exp_compare_data.edw_strt_dttm as edw_strt_dttm,
exp_compare_data.source_record_id
FROM
exp_compare_data
WHERE exp_compare_data.calc_ins_upd = ''I'' OR ( exp_compare_data.Retired = 0 and exp_compare_data.lkp_EDW_END_DTTM != TO_timestamp ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ) OR ( exp_compare_data.calc_ins_upd = ''U'' and exp_compare_data.Trans_Strt_dttm > exp_compare_data.lkp_TRANS_STRT_DTTM1 ));


-- Component rtr_insert_update_flag_Retire, Type ROUTER Output Group Retire
CREATE OR REPLACE TEMPORARY TABLE rtr_insert_update_flag_Retire AS
(SELECT
exp_compare_data.lkp_INDIV_PRTY_ID as lkp_INDIV_PRTY_ID,
exp_compare_data.lkp_NK_PUBLC_ID as lkp_NK_PUBLC_ID,
exp_compare_data.SRC_SYS_CD as SRC_SYS_CD,
exp_compare_data.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_compare_data.in_SRC_STRT_DT as in_SRC_STRT_DT,
exp_compare_data.in_DateOfBirth as in_DateOfBirth,
exp_compare_data.in_GNDR_TYPE_CD as in_GNDR_TYPE_CD,
exp_compare_data.in_TAX_BRAKT_CD as in_TAX_BRAKT_CD,
exp_compare_data.in_NTLTY_CD as in_NTLTY_CD,
exp_compare_data.in_LIFCYL_CD as in_LIFCYL_CD,
exp_compare_data.in_PRTY_TYPE_CD as in_PRTY_TYPE_CD,
exp_compare_data.in_TAX_FILG_TYPE_CD as in_TAX_FILG_TYPE_CD,
exp_compare_data.in_LinkID as in_LinkID,
exp_compare_data.in_TAX_ID_STS_CD as in_TAX_ID_STS_CD,
exp_compare_data.in_PublicID as in_PublicID,
exp_compare_data.in_SRC_END_DT as in_SRC_END_DT,
exp_compare_data.PROCESS_ID as PROCESS_ID,
exp_compare_data.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_compare_data.in_EDW_END_DTTM as in_EDW_END_DTTM,
exp_compare_data.SRC_SYS_CD1 as SRC_SYS_CD4,
exp_compare_data.calc_ins_upd as calc_ins_upd,
exp_compare_data.lkp_NK_LINK_ID as lkp_NK_LINK_ID,
exp_compare_data.in_Source as in_Source,
exp_compare_data.Retired as Retired,
exp_compare_data.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_compare_data.Trans_Strt_dttm as Trans_Strt_dttm,
exp_compare_data.lkp_TRANS_STRT_DTTM1 as lkp_TRANS_STRT_DTTM1,
exp_compare_data.out_PublicIDlkp as out_PublicIDlkp,
exp_compare_data.in_TL_CNT_Name as TL_CNT_Name,
exp_compare_data.Rnk as Rnk,
exp_compare_data.edw_strt_dttm as edw_strt_dttm,
exp_compare_data.source_record_id
FROM
exp_compare_data
WHERE exp_compare_data.calc_ins_upd = ''R'' and exp_compare_data.Retired != 0 and exp_compare_data.lkp_EDW_END_DTTM = TO_timestamp ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ));


-- Component upd_indiv_insert, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_indiv_insert AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_insert_update_flag_Insert.in_SRC_STRT_DT as in_SRC_STRT_DT1,
rtr_insert_update_flag_Insert.in_DateOfBirth as in_DateOfBirth1,
rtr_insert_update_flag_Insert.in_GNDR_TYPE_CD as in_GNDR_TYPE_CD1,
rtr_insert_update_flag_Insert.in_TAX_BRAKT_CD as in_TAX_BRAKT_CD1,
rtr_insert_update_flag_Insert.in_NTLTY_CD as in_NTLTY_CD1,
rtr_insert_update_flag_Insert.in_LIFCYL_CD as in_LIFCYL_CD1,
rtr_insert_update_flag_Insert.in_PRTY_TYPE_CD as in_PRTY_TYPE_CD1,
rtr_insert_update_flag_Insert.in_TAX_FILG_TYPE_CD as in_TAX_FILG_TYPE_CD1,
rtr_insert_update_flag_Insert.in_LinkID as in_LinkID1,
rtr_insert_update_flag_Insert.in_TAX_ID_STS_CD as in_TAX_ID_STS_CD1,
rtr_insert_update_flag_Insert.in_PublicID as in_PublicID1,
rtr_insert_update_flag_Insert.in_SRC_END_DT as in_SRC_END_DT1,
rtr_insert_update_flag_Insert.PROCESS_ID as PROCESS_ID1,
rtr_insert_update_flag_Insert.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM1,
rtr_insert_update_flag_Insert.in_EDW_END_DTTM as in_EDW_END_DTTM1,
rtr_insert_update_flag_Insert.SRC_SYS_CD4 as SRC_SYS_CD,
rtr_insert_update_flag_Insert.Retired as Retired1,
rtr_insert_update_flag_Insert.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM1,
rtr_insert_update_flag_Insert.Trans_Strt_dttm as Trans_Strt_dttm1,
rtr_insert_update_flag_Insert.out_PublicIDlkp as out_PublicIDlkp1,
rtr_insert_update_flag_Insert.in_Source as in_Source1,
rtr_insert_update_flag_Insert.calc_ins_upd as calc_ins_upd1,
rtr_insert_update_flag_Insert.TL_CNT_Name as TL_CNT_Name1,
rtr_insert_update_flag_Insert.Rnk as Rnk1,
rtr_insert_update_flag_Insert.edw_strt_dttm as edw_strt_dttm1,
0 as UPDATE_STRATEGY_ACTION,
rtr_insert_update_flag_Insert.source_record_id
FROM
rtr_insert_update_flag_Insert
);


-- Component RTRRetire_Source_Type1_Update_ClaimCenter, Type ROUTER Output Group Update_ClaimCenter
CREATE OR REPLACE TEMPORARY TABLE RTRRetire_Source_Type1_Update_ClaimCenter AS
(SELECT
rtr_insert_update_flag_Retire.lkp_INDIV_PRTY_ID as lkp_INDIV_PRTY_ID3,
rtr_insert_update_flag_Retire.lkp_NK_PUBLC_ID as lkp_NK_PUBLC_ID3,
rtr_insert_update_flag_Retire.SRC_SYS_CD as SRC_SYS_CD,
rtr_insert_update_flag_Retire.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM3,
rtr_insert_update_flag_Retire.PROCESS_ID as PROCESS_ID3,
rtr_insert_update_flag_Retire.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM3,
rtr_insert_update_flag_Retire.lkp_NK_LINK_ID as lkp_NK_LINK_ID3,
rtr_insert_update_flag_Retire.in_Source as in_Source3,
rtr_insert_update_flag_Retire.Retired as Retired3,
rtr_insert_update_flag_Retire.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM3,
rtr_insert_update_flag_Retire.Trans_Strt_dttm as Trans_Strt_dttm4,
rtr_insert_update_flag_Retire.edw_strt_dttm as edw_strt_dttm2,
rtr_insert_update_flag_Retire.source_record_id
FROM
rtr_insert_update_flag_Retire
WHERE rtr_insert_update_flag_Retire.in_Source = ''ClaimCenter'');


-- Component RTRRetire_Source_Type1_Update_ContactManager, Type ROUTER Output Group Update_ContactManager
CREATE OR REPLACE TEMPORARY TABLE RTRRetire_Source_Type1_Update_ContactManager AS
(SELECT
rtr_insert_update_flag_Retire.lkp_INDIV_PRTY_ID as lkp_INDIV_PRTY_ID3,
rtr_insert_update_flag_Retire.lkp_NK_PUBLC_ID as lkp_NK_PUBLC_ID3,
rtr_insert_update_flag_Retire.SRC_SYS_CD as SRC_SYS_CD,
rtr_insert_update_flag_Retire.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM3,
rtr_insert_update_flag_Retire.PROCESS_ID as PROCESS_ID3,
rtr_insert_update_flag_Retire.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM3,
rtr_insert_update_flag_Retire.lkp_NK_LINK_ID as lkp_NK_LINK_ID3,
rtr_insert_update_flag_Retire.in_Source as in_Source3,
rtr_insert_update_flag_Retire.Retired as Retired3,
rtr_insert_update_flag_Retire.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM3,
rtr_insert_update_flag_Retire.Trans_Strt_dttm as Trans_Strt_dttm4,
rtr_insert_update_flag_Retire.edw_strt_dttm as edw_strt_dttm2,
rtr_insert_update_flag_Retire.source_record_id
FROM
rtr_insert_update_flag_Retire
WHERE rtr_insert_update_flag_Retire.in_Source = ''ContactManager'');


-- Component exp_pass_to_target_insert, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_target_insert AS
(
SELECT
upd_indiv_insert.in_SRC_STRT_DT1 as in_SRC_STRT_DT1,
upd_indiv_insert.in_DateOfBirth1 as in_DateOfBirth1,
upd_indiv_insert.in_GNDR_TYPE_CD1 as in_GNDR_TYPE_CD1,
upd_indiv_insert.in_TAX_BRAKT_CD1 as in_TAX_BRAKT_CD1,
upd_indiv_insert.in_NTLTY_CD1 as in_NTLTY_CD1,
upd_indiv_insert.in_LIFCYL_CD1 as in_LIFCYL_CD1,
upd_indiv_insert.in_PRTY_TYPE_CD1 as in_PRTY_TYPE_CD1,
upd_indiv_insert.in_TAX_FILG_TYPE_CD1 as in_TAX_FILG_TYPE_CD1,
upd_indiv_insert.in_LinkID1 as in_LinkID1,
upd_indiv_insert.in_TAX_ID_STS_CD1 as in_TAX_ID_STS_CD1,
upd_indiv_insert.in_PublicID1 as in_PublicID1,
upd_indiv_insert.in_SRC_END_DT1 as in_SRC_END_DT1,
upd_indiv_insert.PROCESS_ID1 as PROCESS_ID1,
upd_indiv_insert.SRC_SYS_CD as SRC_SYS_CD,
CASE
  WHEN upd_indiv_insert.Retired1 = 0 THEN upd_indiv_insert.in_EDW_END_DTTM1
  ELSE DATEADD (
    SECOND,
    2 * (upd_indiv_insert.Rnk1 - 1),
    upd_indiv_insert.edw_strt_dttm1
  )
END as o_EDW_END_DTTM,
upd_indiv_insert.Trans_Strt_dttm1 as Trans_Strt_dttm1,
CASE
  WHEN upd_indiv_insert.Retired1 != 0 THEN upd_indiv_insert.Trans_Strt_dttm1
  ELSE TO_TIMESTAMP (''9999-12-31 23:59:59.999999'')
END as out_Trans_END_dttm1,
CASE
  WHEN upd_indiv_insert.out_PublicIDlkp1 = :var_prev_public_id
  AND upd_indiv_insert.SRC_SYS_CD = :var_prev_src_system
  AND upd_indiv_insert.calc_ins_upd1 <> ''U'' THEN :va_Prev_prty_id
  WHEN upd_indiv_insert.in_Source1 = ''ContactManager'' THEN LKP_1.PRTY_ID
  WHEN upd_indiv_insert.in_Source1 = ''ClaimCenter'' THEN LKP_2.PRTY_ID
  ELSE 9999
END AS var_INDIV_PRTY_ID,
var_INDIV_PRTY_ID as out_INDIV_PRTY_ID,
upd_indiv_insert.out_PublicIDlkp1 as var_prev_public_id,
upd_indiv_insert.SRC_SYS_CD as var_prev_src_system,
var_INDIV_PRTY_ID as va_Prev_prty_id,
upd_indiv_insert.TL_CNT_Name1 as TL_CNT_Name1,
DATEADD (
  SECOND,
  2 * (upd_indiv_insert.Rnk1 - 1),
  upd_indiv_insert.edw_strt_dttm1
) as out_EDW_STRT_DTTM,
upd_indiv_insert.source_record_id,
row_number() over (partition by upd_indiv_insert.source_record_id order by upd_indiv_insert.source_record_id) as RNK
FROM
upd_indiv_insert
LEFT JOIN LKP_XREF_PRTY_INDIV_CNT_MGR LKP_1 ON LKP_1.NK_LNK_ID = upd_indiv_insert.in_LinkID1
LEFT JOIN LKP_XREF_PRTY_INDIV_CLM_CTR LKP_2 ON LKP_2.NK_PUBLC_ID = upd_indiv_insert.in_PublicID1
QUALIFY RNK = 1
);


-- Component upd_indiv_update_ContactManager1, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_indiv_update_ContactManager1 AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
RTRRetire_Source_Type1_Update_ContactManager.lkp_INDIV_PRTY_ID3 as lkp_INDIV_PRTY_ID3,
RTRRetire_Source_Type1_Update_ContactManager.lkp_NK_LINK_ID3 as lkp_NK_LINK_ID3,
RTRRetire_Source_Type1_Update_ContactManager.SRC_SYS_CD as SRC_SYS_CD,
RTRRetire_Source_Type1_Update_ContactManager.lkp_EDW_STRT_DTTM3 as lkp_EDW_STRT_DTTM3,
RTRRetire_Source_Type1_Update_ContactManager.PROCESS_ID3 as PROCESS_ID3,
RTRRetire_Source_Type1_Update_ContactManager.in_EDW_STRT_DTTM3 as in_EDW_STRT_DTTM3,
RTRRetire_Source_Type1_Update_ContactManager.Retired3 as Retired33,
RTRRetire_Source_Type1_Update_ContactManager.lkp_EDW_END_DTTM3 as lkp_EDW_END_DTTM33,
RTRRetire_Source_Type1_Update_ContactManager.Trans_Strt_dttm4 as Trans_Strt_dttm41,
RTRRetire_Source_Type1_Update_ContactManager.edw_strt_dttm2 as edw_strt_dttm23,
1 as UPDATE_STRATEGY_ACTION,
RTRRetire_Source_Type1_Update_ContactManager.SOURCE_RECORD_ID
FROM
RTRRetire_Source_Type1_Update_ContactManager
);


-- Component upd_indiv_update_ClaimCenter1, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_indiv_update_ClaimCenter1 AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
RTRRetire_Source_Type1_Update_ClaimCenter.lkp_INDIV_PRTY_ID3 as lkp_INDIV_PRTY_ID3,
RTRRetire_Source_Type1_Update_ClaimCenter.lkp_NK_PUBLC_ID3 as lkp_NK_PUBLC_ID3,
RTRRetire_Source_Type1_Update_ClaimCenter.SRC_SYS_CD as SRC_SYS_CD,
RTRRetire_Source_Type1_Update_ClaimCenter.lkp_EDW_STRT_DTTM3 as lkp_EDW_STRT_DTTM3,
RTRRetire_Source_Type1_Update_ClaimCenter.PROCESS_ID3 as PROCESS_ID3,
RTRRetire_Source_Type1_Update_ClaimCenter.in_EDW_STRT_DTTM3 as in_EDW_STRT_DTTM3,
RTRRetire_Source_Type1_Update_ClaimCenter.Retired3 as Retired31,
RTRRetire_Source_Type1_Update_ClaimCenter.lkp_EDW_END_DTTM3 as lkp_EDW_END_DTTM31,
RTRRetire_Source_Type1_Update_ClaimCenter.Trans_Strt_dttm4 as Trans_Strt_dttm41,
RTRRetire_Source_Type1_Update_ClaimCenter.edw_strt_dttm2 as edw_strt_dttm21,
1 as UPDATE_STRATEGY_ACTION,
RTRRetire_Source_Type1_Update_ClaimCenter.SOURCE_RECORD_ID
FROM
RTRRetire_Source_Type1_Update_ClaimCenter
);


-- Component tgt_indiv_insert, Type TARGET 
INSERT INTO DB_T_PROD_CORE.INDIV
(
INDIV_PRTY_ID,
INDIV_STRT_DTTM,
BIRTH_DT,
GNDR_TYPE_CD,
TAX_BRAKT_CD,
NTLTY_CD,
INDIV_END_DTTM,
LIFCYCL_CD,
PRTY_TYPE_CD,
TAX_FILG_TYPE_CD,
NK_LINK_ID,
NK_PUBLC_ID,
TAX_ID_STS_CD,
PRCS_ID,
SRC_SYS_CD,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM,
TRANS_END_DTTM,
INDIV_CTGY_CD
)
SELECT
exp_pass_to_target_insert.out_INDIV_PRTY_ID as INDIV_PRTY_ID,
exp_pass_to_target_insert.in_SRC_STRT_DT1 as INDIV_STRT_DTTM,
exp_pass_to_target_insert.in_DateOfBirth1 as BIRTH_DT,
exp_pass_to_target_insert.in_GNDR_TYPE_CD1 as GNDR_TYPE_CD,
exp_pass_to_target_insert.in_TAX_BRAKT_CD1 as TAX_BRAKT_CD,
exp_pass_to_target_insert.in_NTLTY_CD1 as NTLTY_CD,
exp_pass_to_target_insert.in_SRC_END_DT1 as INDIV_END_DTTM,
exp_pass_to_target_insert.in_LIFCYL_CD1 as LIFCYCL_CD,
exp_pass_to_target_insert.in_PRTY_TYPE_CD1 as PRTY_TYPE_CD,
exp_pass_to_target_insert.in_TAX_FILG_TYPE_CD1 as TAX_FILG_TYPE_CD,
exp_pass_to_target_insert.in_LinkID1 as NK_LINK_ID,
exp_pass_to_target_insert.in_PublicID1 as NK_PUBLC_ID,
exp_pass_to_target_insert.in_TAX_ID_STS_CD1 as TAX_ID_STS_CD,
exp_pass_to_target_insert.PROCESS_ID1 as PRCS_ID,
exp_pass_to_target_insert.SRC_SYS_CD as SRC_SYS_CD,
exp_pass_to_target_insert.out_EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_pass_to_target_insert.o_EDW_END_DTTM as EDW_END_DTTM,
exp_pass_to_target_insert.Trans_Strt_dttm1 as TRANS_STRT_DTTM,
exp_pass_to_target_insert.out_Trans_END_dttm1 as TRANS_END_DTTM,
exp_pass_to_target_insert.TL_CNT_Name1 as INDIV_CTGY_CD
FROM
exp_pass_to_target_insert;


-- Component exp_pass_to_target_update_ContactManager1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_target_update_ContactManager1 AS
(
SELECT
upd_indiv_update_ContactManager1.lkp_INDIV_PRTY_ID3 as lkp_INDIV_PRTY_ID3,
upd_indiv_update_ContactManager1.lkp_NK_LINK_ID3 as lkp_NK_LINK_ID3,
upd_indiv_update_ContactManager1.SRC_SYS_CD as SRC_SYS_CD,
upd_indiv_update_ContactManager1.lkp_EDW_STRT_DTTM3 as lkp_EDW_STRT_DTTM3,
upd_indiv_update_ContactManager1.edw_strt_dttm23 as EDW_END_DTTM,
upd_indiv_update_ContactManager1.Trans_Strt_dttm41 as Trans_Strt_dttm41,
upd_indiv_update_ContactManager1.source_record_id
FROM
upd_indiv_update_ContactManager1
);


-- Component exp_pass_to_target_update_ClaimCenter1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_target_update_ClaimCenter1 AS
(
SELECT
upd_indiv_update_ClaimCenter1.lkp_INDIV_PRTY_ID3 as lkp_INDIV_PRTY_ID3,
upd_indiv_update_ClaimCenter1.lkp_NK_PUBLC_ID3 as lkp_NK_PUBLC_ID3,
upd_indiv_update_ClaimCenter1.SRC_SYS_CD as SRC_SYS_CD,
upd_indiv_update_ClaimCenter1.lkp_EDW_STRT_DTTM3 as lkp_EDW_STRT_DTTM3,
upd_indiv_update_ClaimCenter1.Trans_Strt_dttm41 as Trans_Strt_dttm41,
upd_indiv_update_ClaimCenter1.edw_strt_dttm21 as EDW_END_DTTM,
upd_indiv_update_ClaimCenter1.source_record_id
FROM
upd_indiv_update_ClaimCenter1
);


-- Component tgt_indiv_retire_ContactManager1, Type TARGET 
MERGE INTO DB_T_PROD_CORE.INDIV
USING exp_pass_to_target_update_ContactManager1 ON (INDIV.INDIV_PRTY_ID = exp_pass_to_target_update_ContactManager1.lkp_INDIV_PRTY_ID3 AND INDIV.EDW_STRT_DTTM = exp_pass_to_target_update_ContactManager1.lkp_EDW_STRT_DTTM3)
WHEN MATCHED THEN UPDATE
SET
INDIV_PRTY_ID = exp_pass_to_target_update_ContactManager1.lkp_INDIV_PRTY_ID3,
NK_LINK_ID = exp_pass_to_target_update_ContactManager1.lkp_NK_LINK_ID3,
SRC_SYS_CD = exp_pass_to_target_update_ContactManager1.SRC_SYS_CD,
EDW_STRT_DTTM = exp_pass_to_target_update_ContactManager1.lkp_EDW_STRT_DTTM3,
EDW_END_DTTM = exp_pass_to_target_update_ContactManager1.EDW_END_DTTM,
TRANS_END_DTTM = exp_pass_to_target_update_ContactManager1.Trans_Strt_dttm41;


-- Component tgt_indiv_retire_ClaimCenter1, Type TARGET 
MERGE INTO DB_T_PROD_CORE.INDIV
USING exp_pass_to_target_update_ClaimCenter1 ON (INDIV.INDIV_PRTY_ID = exp_pass_to_target_update_ClaimCenter1.lkp_INDIV_PRTY_ID3 AND INDIV.EDW_STRT_DTTM = exp_pass_to_target_update_ClaimCenter1.lkp_EDW_STRT_DTTM3)
WHEN MATCHED THEN UPDATE
SET
INDIV_PRTY_ID = exp_pass_to_target_update_ClaimCenter1.lkp_INDIV_PRTY_ID3,
NK_PUBLC_ID = exp_pass_to_target_update_ClaimCenter1.lkp_NK_PUBLC_ID3,
SRC_SYS_CD = exp_pass_to_target_update_ClaimCenter1.SRC_SYS_CD,
EDW_STRT_DTTM = exp_pass_to_target_update_ClaimCenter1.lkp_EDW_STRT_DTTM3,
EDW_END_DTTM = exp_pass_to_target_update_ClaimCenter1.EDW_END_DTTM,
TRANS_END_DTTM = exp_pass_to_target_update_ClaimCenter1.Trans_Strt_dttm41;


-- PIPELINE END FOR 1

-- PIPELINE START FOR 2

-- Component sq_ab_abcontact1, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_ab_abcontact1 AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as PublicID,
$2 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT DISTINCT

	/*UPPER(ab_abcontact.LinkID_stg) AS LinkID,

	ab_abcontact.DateOfBirth_stg,

	ab_abcontact.GNDR_TYPE_CD_stg,

	ab_abcontact.TAX_FILG_TYPE_CD_stg,*/

	cast(ab_abcontact.PublicID_stg as varchar(64)) as PublicID_stg

	/*ab_abcontact.SOURCE_stg,

	ab_abcontact.TAX_ID_STS_CD_stg,

	ab_abcontact.SYS_SRC_CD_stg, 

	case when ab_abcontact.createtime_stg is null then TO_DATE(''19000101'',''YYYYMMDD'')   else ab_abcontact.createtime_stg end as SRC_STRT_DT,

	 TIMESTAMP ''9999-12-31 23:59:59.999999'' AS SRC_END_DT,

	retired_stg,

     ab_abcontact.updatetime_stg*/

FROM

	DB_T_PROD_STAG.ab_abcontact

WHERE  

1=2

/* ORDER BY PublicID,LinkID ,updatetime_stg */
) SRC
)
);


-- Component tgt_indiv_postsql, Type TARGET 
INSERT INTO DB_T_PROD_CORE.INDIV
(
INDIV_PRTY_ID
)
SELECT
sq_ab_abcontact1.PublicID as INDIV_PRTY_ID
FROM
sq_ab_abcontact1;


-- PIPELINE END FOR 2
-- Component tgt_indiv_postsql, Type Post SQL 
UPDATE  DB_T_PROD_CORE.INDIV  FROM  

(

SELECT  distinct INDIV_PRTY_ID,EDW_STRT_DTTM,

max(EDW_STRT_DTTM) over (partition by INDIV_PRTY_ID ORDER BY EDW_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND'' 

 as lead1, 

max(TRANS_STRT_DTTM) over (partition by INDIV_PRTY_ID ORDER BY EDW_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND'' 

 as lead

FROM DB_T_PROD_CORE.INDIV 

)  A

set TRANS_END_DTTM=  A.lead, 

EDW_END_DTTM=A.lead1

where  INDIV.EDW_STRT_DTTM = A.EDW_STRT_DTTM

and INDIV.INDIV_PRTY_ID=A.INDIV_PRTY_ID

and INDIV.TRANS_STRT_DTTM <>INDIV.TRANS_END_DTTM

and lead is not null;


END; ';