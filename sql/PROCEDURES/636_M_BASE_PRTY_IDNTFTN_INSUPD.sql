-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_PRTY_IDNTFTN_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
	run_id STRING;
    workflow_name STRING;
    session_name STRING;
    start_dttm TIMESTAMP;
    end_dttm TIMESTAMP;
    PRCS_ID STRING;
	v_start_time TIMESTAMP;
BEGIN
    run_id := (SELECT run_id FROM control_run_id WHERE worklet_name = :worklet_name ORDER BY insert_ts DESC LIMIT 1);
    workflow_name := (SELECT workflow_name FROM control_run_id WHERE worklet_name = :worklet_name ORDER BY insert_ts DESC LIMIT 1);
    session_name := ''s_m_base_prty_idntftn_insupd'';
    start_dttm := public.func_get_scoped_param(:run_id, ''start_dttm'', :workflow_name, :worklet_name, :session_name);
    end_dttm := public.func_get_scoped_param(:run_id, ''end_dttm'', :workflow_name, :worklet_name, :session_name);
    PRCS_ID := public.func_get_scoped_param(:run_id, ''PRCS_ID'', :workflow_name, :worklet_name, :session_name);
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


-- Component LKP_TERADATA_ETL_REF_XLAT, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM IN (''PRTY_IDNTFTN_TYPE'')

	AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM IN (''derived'')

	AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS IN (''DS'')

	AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_BUSN_CTGY_CD, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_BUSN_CTGY_CD AS
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


-- Component sq_ab_abcontact, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_ab_abcontact AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as partyidentificationnum,
$2 as PublicID,
$3 as LinkID,
$4 as prty_idntftn_type_cd,
$5 as Source,
$6 as SYS_SRC_CD,
$7 as createtime,
$8 as Retired,
$9 as TL_CNT_Name,
$10 as updatetime,
$11 as rnk,
$12 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
/* Additional changes for EIM-18200 has been applied during optimization*/


SELECT 

A.partyidentificationnum, A.PublicID,A.LinkID,A.PRTY_IDNTFTN_TYPE_CD,A.SOURCE,A.SYS_SRC_CD,A.CreateTime,A.Retired,A.TL_CNT_Name,A.prty_idntftn_updatetime,

Rank()  OVER(PARTITION BY A.PublicID,A.LinkID,A.PRTY_IDNTFTN_TYPE_CD,A.SOURCE,A.SYS_SRC_CD,A.TL_CNT_Name ORDER BY A.prty_idntftn_updatetime,partyidentificationnum)  as rnk  

FROM (



SELECT DISTINCT

    ab_abcontact.TaxID AS partyidentificationnum,

    ab_abcontact.PublicID AS PublicID,

    ab_abcontact.LinkID AS LinkID,

    case when ab_abcontact.TaxID  LIKE ''__-%'' THEN ''PRTY_IDNTFTN_TYPE11'' 

              when ab_abcontact.TaxID  LIKE ''___-%'' THEN ''PRTY_IDNTFTN_TYPE15''

    end AS PRTY_IDNTFTN_TYPE_CD,

    SOURCE,

    ab_abcontact.SYS_SRC_CD AS SYS_SRC_CD,

      ab_abcontact.createtime,

      ab_abcontact.Retired AS Retired,

      ab_abcontact.TL_CNT_Name,

      case 

    when right(cast(extract(second from  prty_idntftn_updatetime ) as varchar(24)),4) between 1000 and 1499 then cast(cast( prty_idntftn_updatetime  as varchar(22))||''0000'' as timestamp(6)) 

    when right(cast(extract(second from prty_idntftn_updatetime ) as varchar(24)),4) between 1500 and 4499 then cast(cast( prty_idntftn_updatetime  as varchar(22))||''3000'' as timestamp(6))

    when right(cast(extract(second from  prty_idntftn_updatetime ) as varchar(24)),4) between 4500 and 8499 then cast(cast( prty_idntftn_updatetime  as varchar(22))||''7000'' as timestamp(6)) 

    when right(cast(extract(second from  prty_idntftn_updatetime ) as varchar(24)),4) between 8500 and 9999 then cast(cast( prty_idntftn_updatetime  as varchar(22))||''0000'' as timestamp(6)) + INTERVAL ''0.010 SECOND''

    else  prty_idntftn_updatetime 

    end as  prty_idntftn_updatetime 

FROM (

select main_query.TaxID,main_query.PublicID,main_query.LinkID,main_query.Retired,main_query.CreateTime,main_query.LicenseNumber,main_query.TL_CNT_Name,

main_query.Partyidentificationnum,main_query.HICN_alfa,main_query.AdjusterCode_alfa,main_query.SYS_SRC_CD,main_query.PRODUCER_CODE,main_query.SOURCE,main_query.prty_idntftn_updatetime  from (

SELECT  

    bc_contact.TaxID_stg as TaxID,

    bc_contact.PublicID_stg AS PublicID,

    bc_contact.AddressBookUID_stg AS LinkID,

    bc_contact.Retired_stg as Retired,

    bc_contact.CreateTime_stg as CreateTime,

    bc_contact.LicenseNumber_stg as LicenseNumber,

    bctl_contact.NAME_stg AS TL_CNT_Name,

    bc_credential.username_stg AS Partyidentificationnum,

    cast(null as varchar(255)) AS HICN_alfa,

    cast(null as varchar(255)) AS  AdjusterCode_alfa,

    ''SRC_SYS5'' AS SYS_SRC_CD,

    cast(null as varchar(255)) AS PRODUCER_CODE,

    cast(''ClaimCenter'' as varchar(255)) as SOURCE,

    case 

            when (COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))  and

            (COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

            then bc_contact.updatetime_stg

            when (COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE ))) and 

            (COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

            then bc_credential.updatetime_stg

            when (COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))    and 

            (COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

            then bc_user.updatetime_stg

    end as prty_idntftn_updatetime

FROM

    DB_T_PROD_STAG.bc_contact 

    LEFT OUTER JOIN DB_T_PROD_STAG.bctl_contact    ON bctl_contact.id_stg = bc_contact.subtype_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.bctl_gendertype ON bc_contact.gender_stg = bctl_gendertype.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.bctl_taxfilingstatustype ON bc_contact.TaxFilingStatus_stg = bctl_taxfilingstatustype.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.bctl_taxstatus  ON bc_contact.taxstatus_stg = bctl_taxstatus.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.bctl_maritalstatus  ON bc_contact.Maritalstatus_stg = bctl_maritalstatus.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.bctl_nameprefix ON bc_contact.prefix_stg = bctl_nameprefix.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.bctl_namesuffix ON bc_contact.Suffix_stg = bctl_namesuffix.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.bc_user ON bc_user.ContactID_stg = bc_contact.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.bc_credential ON bc_user.CredentialID_stg = bc_credential.id_stg

WHERE   bctl_contact.typecode_stg = (''UserContact'')  

    AND 

/*  below condition added to avoid duplicates*/
       bc_contact.PublicID_stg not in (''default_data:1'',

        ''systemTables:1'',''systemTables:2'') 

    and ((bc_contact.updatetime_stg>(:start_dttm) 

    AND bc_contact.updatetime_stg <=(:end_dttm)) 

    OR (bc_user.updatetime_stg>(:start_dttm) 

    AND bc_user.updatetime_stg <= (:end_dttm)))

        

UNION



SELECT  

    bc_contact.TaxID_stg as TaxID,

    case 

        when(bc_contact.ExternalID_stg is not null) then bc_contact.ExternalID_stg 

        else bc_contact.PublicID_stg 

        end AS PublicID,

    bc_contact.AddressBookUID_stg AS LinkID,

    bc_contact.Retired_stg as Retired,

    bc_contact.CreateTime_stg as CreateTime,

    bc_contact.LicenseNumber_stg as LicenseNumber,

    bctl_contact.NAME_stg AS TL_CNT_Name,

    bc_credential.username_stg AS Partyidentificationnum,

    cast(null as varchar(255)) AS HICN_alfa,

    cast(null as varchar(255)) AS  AdjusterCode_alfa,

    ''SRC_SYS5'' AS SYS_SRC_CD,

    cast(null as varchar(255)) AS PRODUCER_CODE, 

    cast(''ClaimCenter'' as varchar(255)) as SOURCE,

    case 

        when (COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))  and

        (COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then bc_contact.updatetime_stg

        when (COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )))     and 

        (COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then bc_credential.updatetime_stg

        when (COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))    and 

        (COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then bc_user.updatetime_stg

    end as prty_idntftn_updatetime 

from    DB_T_PROD_STAG.bc_account a

inner join DB_T_PROD_STAG.bc_accountcontact h  on h.AccountID_stg = a.id_stg

inner join DB_T_PROD_STAG.bc_contact   on bc_contact.id_stg = h.ContactID_stg

join DB_T_PROD_STAG.bctl_contact   on bctl_contact.id_stg=bc_contact.subtype_stg

left join DB_T_PROD_STAG.bc_accountcontactrole i   on i.AccountContactID_stg = h.id_stg

left join DB_T_PROD_STAG.bctl_accountrole j    on j.id_stg = i.Role_stg

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_gendertype     ON bc_contact.gender_stg = bctl_gendertype.id_stg 

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_taxfilingstatustype    ON bc_contact.TaxFilingStatus_stg = bctl_taxfilingstatustype.id_stg 

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_taxstatus  ON bc_contact.taxstatus_stg = bctl_taxstatus.id_stg

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_maritalstatus  ON bc_contact.Maritalstatus_stg = bctl_maritalstatus.id_stg 

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_nameprefix     ON bc_contact.prefix_stg = bctl_nameprefix.id_stg 

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_namesuffix     ON bc_contact.Suffix_stg = bctl_namesuffix.id_stg

LEFT OUTER JOIN DB_T_PROD_STAG.bc_user     ON bc_user.ContactID_stg = bc_contact.id_stg

LEFT OUTER JOIN DB_T_PROD_STAG.bc_credential   on bc_user.CredentialID_stg = bc_credential.id_stg

where   ((h.PrimaryPayer_stg = 1) 

    or (j.NAME_stg = ''Payer''))

    and ((bc_contact.updatetime_stg>(:start_dttm) 

    AND bc_contact.updatetime_stg <=(:end_dttm)) 

    OR (bc_user.updatetime_stg>(:start_dttm) 

    AND bc_user.updatetime_stg <= (:end_dttm)))



UNION

/*    Primary Payer and Overiding Payer Contact (this is at the Invoicestream level)*/
SELECT  

    bc_contact.TaxID_stg as TaxID,

    case 

        when (bc_contact.ExternalID_stg is null) then bc_contact.PublicID_stg 

        else bc_contact.ExternalID_stg 

        end AS PublicID,

    bc_contact.AddressBookUID_stg AS LinkID,

    bc_contact.Retired_stg as Retired,

    bc_contact.CreateTime_stg as CreateTime,

    bc_contact.LicenseNumber_stg as LicenseNumber,

    bctl_contact.NAME_stg AS TL_CNT_Name,

    bc_credential.username_stg AS Partyidentificationnum,

    cast(null as varchar(255)) AS HICN_alfa,

    cast(null as varchar(255)) AS  AdjusterCode_alfa,

    ''SRC_SYS5'' AS SYS_SRC_CD,

    cast(null as varchar(255)) AS PRODUCER_CODE,

    cast(''ClaimCenter'' as varchar(255)) as SOURCE,

    case 

        when (COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))  and

        (COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then bc_contact.updatetime_stg

        when (COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )))     and 

        (COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then bc_credential.updatetime_stg

        when (COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))    and 

        (COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then bc_user.updatetime_stg

    end as prty_idntftn_updatetime

from    DB_T_PROD_STAG.bc_account a

inner join DB_T_PROD_STAG.bc_invoicestream b   on a.id_stg = b.AccountID_stg

inner join DB_T_PROD_STAG.bc_accountcontact c  on c.AccountID_stg=a.id_stg

inner join DB_T_PROD_STAG.bc_contact   on bc_contact.id_stg = c.ContactID_stg

join DB_T_PROD_STAG.bctl_contact   on bctl_contact.id_stg=bc_contact.subtype_stg

left join DB_T_PROD_STAG.bc_accountcontactrole f   on f.AccountContactID_stg = c.id_stg

left join DB_T_PROD_STAG.bctl_accountrole g    on g.id_stg = f.Role_stg

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_gendertype     ON bc_contact.gender_stg = bctl_gendertype.id_stg 

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_taxfilingstatustype    ON bc_contact.TaxFilingStatus_stg = bctl_taxfilingstatustype.id_stg 

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_taxstatus  ON bc_contact.taxstatus_stg = bctl_taxstatus.id_stg

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_maritalstatus  ON bc_contact.Maritalstatus_stg = bctl_maritalstatus.id_stg 

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_nameprefix     ON bc_contact.prefix_stg = bctl_nameprefix.id_stg 

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_namesuffix     ON bc_contact.Suffix_stg = bctl_namesuffix.id_stg

LEFT OUTER JOIN DB_T_PROD_STAG.bc_user     ON bc_user.ContactID_stg = bc_contact.id_stg

LEFT OUTER JOIN DB_T_PROD_STAG.bc_credential   on bc_user.CredentialID_stg = bc_credential.id_stg

where   ((b.OverridingPayer_alfa_stg is null 

    and c.PrimaryPayer_stg = 1) 

    or (b.OverridingPayer_alfa_stg is not null))

    and ((bc_contact.updatetime_stg>(:start_dttm) 

    AND bc_contact.updatetime_stg <=(:end_dttm)) 

    OR (bc_user.updatetime_stg>(:start_dttm) 

    AND bc_user.updatetime_stg <= (:end_dttm)))



UNION



SELECT  

    pc_contact.TaxID_stg as TaxID,

    pc_contact.PublicID_stg as PublicID,

    pc_contact.AddressBookUID_stg AS LinkID,

    pc_contact.Retired_stg as Retired,

    pc_contact.CreateTime_stg as CreateTime,

    pc_contact.LicenseNumber_stg as LicenseNumber,

    pctl_contact.NAME_stg AS TL_CNT_Name,

    pc_credential.username_stg AS Partyidentificationnum,

    cast(null as varchar(255)) AS HICN_alfa,

    cast(null as varchar(255)) as AdjusterCode_alfa,

/* pc_producercode.code as agentnumber, */
    ''SRC_SYS4'' AS SYS_SRC_CD,

    cast(null as varchar(255)) as PRODUCER_CODE,

    cast(''ClaimCenter'' as varchar(255)) as SOURCE,

    case 

        when (COALESCE(pc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(pc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))  and

        (COALESCE(pc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(pc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then pc_contact.updatetime_stg

        when (COALESCE(pc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(pc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )))     and 

        (COALESCE(pc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(pc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then pc_credential.updatetime_stg

        when (COALESCE(pc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(pc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))    and 

        (COALESCE(pc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(pc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then pc_user.updatetime_stg

    end as prty_idntftn_updatetime 

FROM

    DB_T_PROD_STAG.pc_contact 

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_contact    ON pctl_contact.id_stg = pc_contact.subtype_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_gendertype     ON pc_contact.gender_stg = pctl_gendertype.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_taxfilingstatustype    ON pc_contact.TaxFilingStatus_stg = pctl_taxfilingstatustype.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_taxstatus  ON pc_contact.taxstatus_stg = pctl_taxstatus.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_maritalstatus  ON pc_contact.Maritalstatus_stg = pctl_maritalstatus.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_nameprefix     ON pc_contact.prefix_stg = pctl_nameprefix.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_namesuffix     ON pc_contact.Suffix_stg = pctl_namesuffix.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.pc_user     ON pc_user.ContactID_stg = pc_contact.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.pc_credential   on pc_user.CredentialID_stg = pc_credential.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod     on pc_policyperiod.PNIContactDenorm_stg = pc_contact.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.pc_effectivedatedfields     on pc_effectivedatedfields.BranchID_stg = pc_policyperiod.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.pc_producercode     on pc_producercode.id_stg=pc_effectivedatedfields.ProducerCodeID_stg

WHERE

    pctl_contact.typecode_stg = (''UserContact'')  

    AND 

/*  below condition added to avoid duplicates*/
       pc_contact.PublicID_stg not in (''default_data:1'',

        ''systemTables:1'',''systemTables:2'') 

     AND ((pc_contact.updatetime_stg>(:start_dttm) 

    AND pc_contact.updatetime_stg <= (:end_dttm)) 

    OR (pc_user.updatetime_stg>(:start_dttm) 

    AND pc_user.updatetime_stg <= (:end_dttm)))

     

UNION



/* DB_T_PROD_STAG.CC_CONTACT */
SELECT  DISTINCT 

    cc_contact.TaxID_stg as TaxID,

    cc_contact.PublicID_stg as PublicID,

    cc_contact.AddressBookUID_stg AS LinkID,

    cc_contact.Retired_stg as Retired,

    cc_contact.CreateTime_stg as CreateTime,

    cc_contact.LicenseNumber_stg as LicenseNumber,

    cctl_contact.NAME_stg AS TL_CNT_Name,

    cc_credential.username_stg AS Partyidentificationnum,

/* cc_incident.HICN_alfa_stg AS HICN_alfa,*/
    cc_contact.HICN_alfa_stg AS HICN_alfa,

    cc_user.AdjusterCode_alfa_stg as AdjusterCode_alfa,

    ''SRC_SYS6'' AS SYS_SRC_CD,

    cast(null as varchar(255)) as PRODUCER_CODE,

    cast(''ClaimCenter'' as varchar(255)) as SOURCE,

    case 

        when (COALESCE(cc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(cc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))  and

        (COALESCE(cc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(cc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then cc_contact.updatetime_stg

        when (COALESCE(cc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(cc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )))     and 

        (COALESCE(cc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(cc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then cc_credential.updatetime_stg

        when (COALESCE(cc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(cc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))    and 

        (COALESCE(cc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(cc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then cc_user.updatetime_stg

    end as prty_idntftn_updatetime 



FROM

    DB_T_PROD_STAG.CC_CONTACT 

    LEFT OUTER JOIN DB_T_PROD_STAG.cctl_contact    ON cctl_contact.id_stg = cc_contact.subtype_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.cctl_gendertype     ON cc_contact.gender_stg = cctl_gendertype.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.cctl_taxfilingstatustype    ON cc_contact.TaxFilingStatus_stg = cctl_taxfilingstatustype.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.cctl_taxstatus  ON cc_contact.taxstatus_stg = cctl_taxstatus.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.cctl_maritalstatus  ON cc_contact.Maritalstatus_stg = cctl_maritalstatus.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.cctl_nameprefix     ON cc_contact.prefix_stg = cctl_nameprefix.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.cctl_namesuffix     ON cc_contact.Suffix_stg = cctl_namesuffix.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.cc_user     ON cc_user.ContactID_stg = cc_contact.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.cc_credential   ON cc_user.CredentialID_stg = cc_credential.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.cc_claimcontact ON cc_contact.id_stg=cc_claimcontact.ContactID_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.cc_claimcontactrole ON cc_claimcontact.id_stg=cc_claimcontactrole.claimcontactid_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.cc_incident     ON cc_claimcontactrole.claimcontactid_stg =cc_incident.id_stg

WHERE

    (cc_contact.updatetime_stg>(:start_dttm) 

    AND cc_contact.updatetime_stg <= (:end_dttm) )  

    or 

       (cc_user.updatetime_stg>(:start_dttm) 

    AND cc_user.updatetime_stg <=(:end_dttm) )

      

UNION



SELECT  

    ab_abcontact.TaxID_stg as TaxID,

    cast(null as varchar(255)) AS PublicID,

    ab_abcontact.LinkID_stg as LinkID,

    ab_abcontact.Retired_stg as Retired,

    ab_abcontact.CreateTime_stg as CreateTime,

    ab_abcontact.LicenseNumber_stg as LicenseNumber,

    abtl_abcontact.NAME_stg AS TL_CNT_Name,

    ab_credential.username_stg AS Partyidentificationnum,

    cast(null as varchar(255)) AS HICN_alfa,

    cast(null as varchar(255)) as AdjusterCode_alfa, 

/* abtl_occupation.typecode_stg,*/
    ''SRC_SYS7'' AS SYS_SRC_CD,

    LKP_Agent_Code.Code as PRODUCER_CODE,

    cast(''ContactManager'' as varchar(255)) as SOURCE,

    case 

        when (COALESCE(ab_abcontact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(ab_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))    and

        (COALESCE(ab_abcontact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(ab_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then ab_abcontact.updatetime_stg

        when (COALESCE(ab_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(ab_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )))     and 

        (COALESCE(ab_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(ab_abcontact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then ab_credential.updatetime_stg

        when (COALESCE(ab_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(ab_abcontact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))  and 

        (COALESCE(ab_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(ab_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then ab_user.updatetime_stg

    end as prty_idntftn_updatetime 

FROM

    DB_T_PROD_STAG.ab_abcontact

    LEFT OUTER JOIN DB_T_PROD_STAG.abtl_abcontact  ON abtl_abcontact.id_stg = ab_abcontact.subtype_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.abtl_gendertype     ON ab_abcontact.gender_stg = abtl_gendertype.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.abtl_taxfilingstatustype    ON ab_abcontact.TaxFilingStatus_stg = abtl_taxfilingstatustype.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.abtl_taxstatus  ON ab_abcontact.taxstatus_stg = abtl_taxstatus.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.abtl_maritalstatus  ON ab_abcontact.Maritalstatus_stg = abtl_maritalstatus.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.abtl_nameprefix     ON ab_abcontact.prefix_stg = abtl_nameprefix.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.abtl_namesuffix     ON ab_abcontact.Suffix_stg = abtl_namesuffix.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.ab_user     ON ab_user.ContactID_stg = ab_abcontact.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.ab_credential   on ab_user.CredentialID_stg = ab_credential.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.abtl_occupation     ON  abtl_occupation.id_stg = ab_abcontact.occupation_alfa_stg

    LEFT OUTER JOIN (   select   contact.AddressBookUID_stg AS AddressBookUID,  producer_cd.Code_stg AS Code  

    from     DB_T_PROD_STAG.pc_producercode producer_cd    

    join DB_T_PROD_STAG.pc_userproducercode user_producer      on (user_producer.ProducerCodeID_stg = producer_cd.id_stg)

    join DB_T_PROD_STAG.pc_user  user1         on(user1.id_stg= user_producer.UserID_stg)

    join DB_T_PROD_STAG.pc_contact contact         on(contact.id_stg = user1.ContactID_stg )    ) AS LKP_Agent_Code 

    on LKP_Agent_Code.AddressBookUID = ab_abcontact.LinkID_stg

    WHERE ab_abcontact.updatetime_stg>(:start_dttm) AND ab_abcontact.updatetime_stg <= (:end_dttm)

    ) main_query  )

    ab_abcontact 

 WHERE 

/* ab_abcontact.TL_CNT_Name IN (''Person'',''Adjudicator'',''User Contact'',''Vendor (Person)'',''Attorney'', ''Doctor'',''Policy Person'') AND*/ 
     ((SOURCE = ''ClaimCenter'' AND PublicID IS NOT NULL) OR (SOURCE = ''ContactManager'' AND LinkID IS NOT NULL)) AND

/* licensenumber IS  NULL AND      --PRODUCER_CODE IS NULL AND */
    TaxID  IS NOT NULL







UNION



SELECT  DISTINCT

    ab_abcontact.licensenumber AS partyidentificationnum,

    ab_abcontact.PublicID AS PublicID,

    ab_abcontact.LinkID AS LinkID,

    ''PRTY_IDNTFTN_TYPE5'' AS PRTY_IDNTFTN_TYPE_CD,

    SOURCE,

    ab_abcontact.SYS_SRC_CD AS SYS_SRC_CD,

      ab_abcontact.createtime,

      ab_abcontact.Retired AS Retired,

      ab_abcontact.TL_CNT_Name,

      case 

    when right(cast(extract(second from  prty_idntftn_updatetime ) as varchar(24)),4) between 1000 and 1499 then cast(cast( prty_idntftn_updatetime  as varchar(22))||''0000'' as timestamp(6)) 

    when right(cast(extract(second from prty_idntftn_updatetime ) as varchar(24)),4) between 1500 and 4499 then cast(cast( prty_idntftn_updatetime  as varchar(22))||''3000'' as timestamp(6))

    when right(cast(extract(second from  prty_idntftn_updatetime ) as varchar(24)),4) between 4500 and 8499 then cast(cast( prty_idntftn_updatetime  as varchar(22))||''7000'' as timestamp(6)) 

    when right(cast(extract(second from  prty_idntftn_updatetime ) as varchar(24)),4) between 8500 and 9999 then cast(cast( prty_idntftn_updatetime  as varchar(22))||''0000'' as timestamp(6)) + INTERVAL ''0.010 SECOND''

    else  prty_idntftn_updatetime 

    end as  prty_idntftn_updatetime 

FROM  (



select main_query.TaxID,main_query.PublicID,main_query.LinkID,main_query.Retired,main_query.CreateTime,main_query.LicenseNumber,main_query.TL_CNT_Name,

main_query.Partyidentificationnum,main_query.HICN_alfa,main_query.AdjusterCode_alfa,main_query.SYS_SRC_CD,main_query.PRODUCER_CODE,main_query.SOURCE,main_query.prty_idntftn_updatetime  from (

SELECT  

    bc_contact.TaxID_stg as TaxID,

    bc_contact.PublicID_stg AS PublicID,

    bc_contact.AddressBookUID_stg AS LinkID,

    bc_contact.Retired_stg as Retired,

    bc_contact.CreateTime_stg as CreateTime,

    bc_contact.LicenseNumber_stg as LicenseNumber,

    bctl_contact.NAME_stg AS TL_CNT_Name,

    bc_credential.username_stg AS Partyidentificationnum,

    cast(null as varchar(255)) AS HICN_alfa,

    cast(null as varchar(255)) AS  AdjusterCode_alfa,

    ''SRC_SYS5'' AS SYS_SRC_CD,

    cast(null as varchar(255)) AS PRODUCER_CODE,

    cast(''ClaimCenter'' as varchar(255)) as SOURCE,

    case 

            when (COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))  and

            (COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

            then bc_contact.updatetime_stg

            when (COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE ))) and 

            (COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

            then bc_credential.updatetime_stg

            when (COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))    and 

            (COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

            then bc_user.updatetime_stg

    end as prty_idntftn_updatetime

FROM

    DB_T_PROD_STAG.bc_contact 

    LEFT OUTER JOIN DB_T_PROD_STAG.bctl_contact    ON bctl_contact.id_stg = bc_contact.subtype_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.bctl_gendertype ON bc_contact.gender_stg = bctl_gendertype.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.bctl_taxfilingstatustype ON bc_contact.TaxFilingStatus_stg = bctl_taxfilingstatustype.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.bctl_taxstatus  ON bc_contact.taxstatus_stg = bctl_taxstatus.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.bctl_maritalstatus  ON bc_contact.Maritalstatus_stg = bctl_maritalstatus.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.bctl_nameprefix ON bc_contact.prefix_stg = bctl_nameprefix.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.bctl_namesuffix ON bc_contact.Suffix_stg = bctl_namesuffix.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.bc_user ON bc_user.ContactID_stg = bc_contact.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.bc_credential ON bc_user.CredentialID_stg = bc_credential.id_stg

WHERE   bctl_contact.typecode_stg = (''UserContact'')  

    AND 

/*  below condition added to avoid duplicates*/
       bc_contact.PublicID_stg not in (''default_data:1'',

        ''systemTables:1'',''systemTables:2'') 

    and ((bc_contact.updatetime_stg>(:start_dttm) 

    AND bc_contact.updatetime_stg <=(:end_dttm)) 

    OR (bc_user.updatetime_stg>(:start_dttm) 

    AND bc_user.updatetime_stg <= (:end_dttm)))

        

UNION



SELECT  

    bc_contact.TaxID_stg as TaxID,

    case 

        when(bc_contact.ExternalID_stg is not null) then bc_contact.ExternalID_stg 

        else bc_contact.PublicID_stg 

        end AS PublicID,

    bc_contact.AddressBookUID_stg AS LinkID,

    bc_contact.Retired_stg as Retired,

    bc_contact.CreateTime_stg as CreateTime,

    bc_contact.LicenseNumber_stg as LicenseNumber,

    bctl_contact.NAME_stg AS TL_CNT_Name,

    bc_credential.username_stg AS Partyidentificationnum,

    cast(null as varchar(255)) AS HICN_alfa,

    cast(null as varchar(255)) AS  AdjusterCode_alfa,

    ''SRC_SYS5'' AS SYS_SRC_CD,

    cast(null as varchar(255)) AS PRODUCER_CODE, 

    cast(''ClaimCenter'' as varchar(255)) as SOURCE,

    case 

        when (COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))  and

        (COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then bc_contact.updatetime_stg

        when (COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )))     and 

        (COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then bc_credential.updatetime_stg

        when (COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))    and 

        (COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then bc_user.updatetime_stg

    end as prty_idntftn_updatetime 

from    DB_T_PROD_STAG.bc_account a

inner join DB_T_PROD_STAG.bc_accountcontact h  on h.AccountID_stg = a.id_stg

inner join DB_T_PROD_STAG.bc_contact   on bc_contact.id_stg = h.ContactID_stg

join DB_T_PROD_STAG.bctl_contact   on bctl_contact.id_stg=bc_contact.subtype_stg

left join DB_T_PROD_STAG.bc_accountcontactrole i   on i.AccountContactID_stg = h.id_stg

left join DB_T_PROD_STAG.bctl_accountrole j    on j.id_stg = i.Role_stg

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_gendertype     ON bc_contact.gender_stg = bctl_gendertype.id_stg 

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_taxfilingstatustype    ON bc_contact.TaxFilingStatus_stg = bctl_taxfilingstatustype.id_stg 

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_taxstatus  ON bc_contact.taxstatus_stg = bctl_taxstatus.id_stg

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_maritalstatus  ON bc_contact.Maritalstatus_stg = bctl_maritalstatus.id_stg 

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_nameprefix     ON bc_contact.prefix_stg = bctl_nameprefix.id_stg 

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_namesuffix     ON bc_contact.Suffix_stg = bctl_namesuffix.id_stg

LEFT OUTER JOIN DB_T_PROD_STAG.bc_user     ON bc_user.ContactID_stg = bc_contact.id_stg

LEFT OUTER JOIN DB_T_PROD_STAG.bc_credential   on bc_user.CredentialID_stg = bc_credential.id_stg

where   ((h.PrimaryPayer_stg = 1) 

    or (j.NAME_stg = ''Payer''))

    and ((bc_contact.updatetime_stg>(:start_dttm) 

    AND bc_contact.updatetime_stg <=(:end_dttm)) 

    OR (bc_user.updatetime_stg>(:start_dttm) 

    AND bc_user.updatetime_stg <= (:end_dttm)))



UNION

/*    Primary Payer and Overiding Payer Contact (this is at the Invoicestream level)*/
SELECT  

    bc_contact.TaxID_stg as TaxID,

    case 

        when (bc_contact.ExternalID_stg is null) then bc_contact.PublicID_stg 

        else bc_contact.ExternalID_stg 

        end AS PublicID,

    bc_contact.AddressBookUID_stg AS LinkID,

    bc_contact.Retired_stg as Retired,

    bc_contact.CreateTime_stg as CreateTime,

    bc_contact.LicenseNumber_stg as LicenseNumber,

    bctl_contact.NAME_stg AS TL_CNT_Name,

    bc_credential.username_stg AS Partyidentificationnum,

    cast(null as varchar(255)) AS HICN_alfa,

    cast(null as varchar(255)) AS  AdjusterCode_alfa,

    ''SRC_SYS5'' AS SYS_SRC_CD,

    cast(null as varchar(255)) AS PRODUCER_CODE,

    cast(''ClaimCenter'' as varchar(255)) as SOURCE,

    case 

        when (COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))  and

        (COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then bc_contact.updatetime_stg

        when (COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )))     and 

        (COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then bc_credential.updatetime_stg

        when (COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))    and 

        (COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then bc_user.updatetime_stg

    end as prty_idntftn_updatetime 

from    DB_T_PROD_STAG.bc_account a

inner join DB_T_PROD_STAG.bc_invoicestream b   on a.id_stg = b.AccountID_stg

inner join DB_T_PROD_STAG.bc_accountcontact c  on c.AccountID_stg=a.id_stg

inner join DB_T_PROD_STAG.bc_contact   on bc_contact.id_stg = c.ContactID_stg

join DB_T_PROD_STAG.bctl_contact   on bctl_contact.id_stg=bc_contact.subtype_stg

left join DB_T_PROD_STAG.bc_accountcontactrole f   on f.AccountContactID_stg = c.id_stg

left join DB_T_PROD_STAG.bctl_accountrole g    on g.id_stg = f.Role_stg

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_gendertype     ON bc_contact.gender_stg = bctl_gendertype.id_stg 

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_taxfilingstatustype    ON bc_contact.TaxFilingStatus_stg = bctl_taxfilingstatustype.id_stg 

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_taxstatus  ON bc_contact.taxstatus_stg = bctl_taxstatus.id_stg

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_maritalstatus  ON bc_contact.Maritalstatus_stg = bctl_maritalstatus.id_stg 

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_nameprefix     ON bc_contact.prefix_stg = bctl_nameprefix.id_stg 

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_namesuffix     ON bc_contact.Suffix_stg = bctl_namesuffix.id_stg

LEFT OUTER JOIN DB_T_PROD_STAG.bc_user     ON bc_user.ContactID_stg = bc_contact.id_stg

LEFT OUTER JOIN DB_T_PROD_STAG.bc_credential   on bc_user.CredentialID_stg = bc_credential.id_stg

where   ((b.OverridingPayer_alfa_stg is null 

    and c.PrimaryPayer_stg = 1) 

    or (b.OverridingPayer_alfa_stg is not null))

    and ((bc_contact.updatetime_stg>(:start_dttm) 

    AND bc_contact.updatetime_stg <=(:end_dttm)) 

    OR (bc_user.updatetime_stg>(:start_dttm) 

    AND bc_user.updatetime_stg <= (:end_dttm)))



UNION



SELECT  

    pc_contact.TaxID_stg as TaxID,

    pc_contact.PublicID_stg as PublicID,

    pc_contact.AddressBookUID_stg AS LinkID,

    pc_contact.Retired_stg as Retired,

    pc_contact.CreateTime_stg as CreateTime,

    pc_contact.LicenseNumber_stg as LicenseNumber,

    pctl_contact.NAME_stg AS TL_CNT_Name,

    pc_credential.username_stg AS Partyidentificationnum,

    cast(null as varchar(255)) AS HICN_alfa,

    cast(null as varchar(255)) as AdjusterCode_alfa,

/*  pc_producercode.code as agentnumber, */
    ''SRC_SYS4'' AS SYS_SRC_CD,

    cast(null as varchar(255)) as PRODUCER_CODE,

    cast(''ClaimCenter'' as varchar(255)) as SOURCE,

    case 

        when (COALESCE(pc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(pc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))  and

        (COALESCE(pc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(pc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then pc_contact.updatetime_stg

        when (COALESCE(pc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(pc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )))     and 

        (COALESCE(pc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(pc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then pc_credential.updatetime_stg

        when (COALESCE(pc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(pc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))    and 

        (COALESCE(pc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(pc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then pc_user.updatetime_stg

    end as prty_idntftn_updatetime  

FROM

    DB_T_PROD_STAG.pc_contact 

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_contact    ON pctl_contact.id_stg = pc_contact.subtype_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_gendertype     ON pc_contact.gender_stg = pctl_gendertype.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_taxfilingstatustype    ON pc_contact.TaxFilingStatus_stg = pctl_taxfilingstatustype.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_taxstatus  ON pc_contact.taxstatus_stg = pctl_taxstatus.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_maritalstatus  ON pc_contact.Maritalstatus_stg = pctl_maritalstatus.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_nameprefix     ON pc_contact.prefix_stg = pctl_nameprefix.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_namesuffix     ON pc_contact.Suffix_stg = pctl_namesuffix.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.pc_user     ON pc_user.ContactID_stg = pc_contact.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.pc_credential   on pc_user.CredentialID_stg = pc_credential.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod     on pc_policyperiod.PNIContactDenorm_stg = pc_contact.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.pc_effectivedatedfields     on pc_effectivedatedfields.BranchID_stg = pc_policyperiod.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.pc_producercode     on pc_producercode.id_stg=pc_effectivedatedfields.ProducerCodeID_stg

WHERE

    pctl_contact.typecode_stg = (''UserContact'')  

    AND 

/*  below condition added to avoid duplicates*/
       pc_contact.PublicID_stg not in (''default_data:1'',

        ''systemTables:1'',''systemTables:2'') 

     AND ((pc_contact.updatetime_stg>(:start_dttm) 

    AND pc_contact.updatetime_stg <= (:end_dttm)) 

    OR (pc_user.updatetime_stg>(:start_dttm) 

    AND pc_user.updatetime_stg <= (:end_dttm)))

     

UNION



/* DB_T_PROD_STAG.CC_CONTACT */
SELECT  DISTINCT 

    cc_contact.TaxID_stg as TaxID,

    cc_contact.PublicID_stg as PublicID,

    cc_contact.AddressBookUID_stg AS LinkID,

    cc_contact.Retired_stg as Retired,

    cc_contact.CreateTime_stg as CreateTime,

    cc_contact.LicenseNumber_stg as LicenseNumber,

    cctl_contact.NAME_stg AS TL_CNT_Name,

    cc_credential.username_stg AS Partyidentificationnum,

/* cc_incident.HICN_alfa_stg AS HICN_alfa,*/
    cc_contact.HICN_alfa_stg AS HICN_alfa,

    cc_user.AdjusterCode_alfa_stg as AdjusterCode_alfa,

    ''SRC_SYS6'' AS SYS_SRC_CD,

    cast(null as varchar(255)) as PRODUCER_CODE,

    cast(''ClaimCenter'' as varchar(255)) as SOURCE,

    case 

        when (COALESCE(cc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(cc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))  and

        (COALESCE(cc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(cc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then cc_contact.updatetime_stg

        when (COALESCE(cc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(cc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )))     and 

        (COALESCE(cc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(cc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then cc_credential.updatetime_stg

        when (COALESCE(cc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(cc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))    and 

        (COALESCE(cc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(cc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then cc_user.updatetime_stg

    end as prty_idntftn_updatetime 



FROM

    DB_T_PROD_STAG.CC_CONTACT 

    LEFT OUTER JOIN DB_T_PROD_STAG.cctl_contact    ON cctl_contact.id_stg = cc_contact.subtype_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.cctl_gendertype     ON cc_contact.gender_stg = cctl_gendertype.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.cctl_taxfilingstatustype    ON cc_contact.TaxFilingStatus_stg = cctl_taxfilingstatustype.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.cctl_taxstatus  ON cc_contact.taxstatus_stg = cctl_taxstatus.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.cctl_maritalstatus  ON cc_contact.Maritalstatus_stg = cctl_maritalstatus.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.cctl_nameprefix     ON cc_contact.prefix_stg = cctl_nameprefix.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.cctl_namesuffix     ON cc_contact.Suffix_stg = cctl_namesuffix.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.cc_user     ON cc_user.ContactID_stg = cc_contact.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.cc_credential   ON cc_user.CredentialID_stg = cc_credential.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.cc_claimcontact ON cc_contact.id_stg=cc_claimcontact.ContactID_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.cc_claimcontactrole ON cc_claimcontact.id_stg=cc_claimcontactrole.claimcontactid_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.cc_incident     ON cc_claimcontactrole.claimcontactid_stg =cc_incident.id_stg

WHERE

    (cc_contact.updatetime_stg>(:start_dttm) 

    AND cc_contact.updatetime_stg <= (:end_dttm) )  

    or 

       (cc_user.updatetime_stg>(:start_dttm) 

    AND cc_user.updatetime_stg <=(:end_dttm) )

      

UNION



SELECT  

    ab_abcontact.TaxID_stg as TaxID,

    cast(null as varchar(255)) AS PublicID,

    ab_abcontact.LinkID_stg as LinkID,  

    ab_abcontact.Retired_stg as Retired,

    ab_abcontact.CreateTime_stg as CreateTime,

    ab_abcontact.LicenseNumber_stg as LicenseNumber,

    abtl_abcontact.NAME_stg AS TL_CNT_Name,

    ab_credential.username_stg AS Partyidentificationnum,

    cast(null as varchar(255)) AS HICN_alfa,

    cast(null as varchar(255)) as AdjusterCode_alfa, 

/* abtl_occupation.typecode_stg,*/
    ''SRC_SYS7'' AS SYS_SRC_CD,

    LKP_Agent_Code.Code as PRODUCER_CODE,

    cast(''ContactManager'' as varchar(255)) as SOURCE,

    case 

        when (COALESCE(ab_abcontact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(ab_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))    and

        (COALESCE(ab_abcontact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(ab_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then ab_abcontact.updatetime_stg

        when (COALESCE(ab_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(ab_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )))     and 

        (COALESCE(ab_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(ab_abcontact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then ab_credential.updatetime_stg

        when (COALESCE(ab_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(ab_abcontact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))  and 

        (COALESCE(ab_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(ab_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then ab_user.updatetime_stg

    end as prty_idntftn_updatetime 

FROM

    DB_T_PROD_STAG.ab_abcontact

    LEFT OUTER JOIN DB_T_PROD_STAG.abtl_abcontact  ON abtl_abcontact.id_stg = ab_abcontact.subtype_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.abtl_gendertype     ON ab_abcontact.gender_stg = abtl_gendertype.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.abtl_taxfilingstatustype    ON ab_abcontact.TaxFilingStatus_stg = abtl_taxfilingstatustype.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.abtl_taxstatus  ON ab_abcontact.taxstatus_stg = abtl_taxstatus.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.abtl_maritalstatus  ON ab_abcontact.Maritalstatus_stg = abtl_maritalstatus.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.abtl_nameprefix     ON ab_abcontact.prefix_stg = abtl_nameprefix.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.abtl_namesuffix     ON ab_abcontact.Suffix_stg = abtl_namesuffix.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.ab_user     ON ab_user.ContactID_stg = ab_abcontact.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.ab_credential   on ab_user.CredentialID_stg = ab_credential.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.abtl_occupation     ON  abtl_occupation.id_stg = ab_abcontact.occupation_alfa_stg

    LEFT OUTER JOIN (   select   contact.AddressBookUID_stg AS AddressBookUID,  producer_cd.Code_stg AS Code  

    from     DB_T_PROD_STAG.pc_producercode producer_cd    

    join DB_T_PROD_STAG.pc_userproducercode user_producer      on (user_producer.ProducerCodeID_stg = producer_cd.id_stg)

    join DB_T_PROD_STAG.pc_user  user1         on(user1.id_stg= user_producer.UserID_stg)

    join DB_T_PROD_STAG.pc_contact contact         on(contact.id_stg = user1.ContactID_stg )    ) AS LKP_Agent_Code 

    on LKP_Agent_Code.AddressBookUID = ab_abcontact.LinkID_stg

    WHERE ab_abcontact.updatetime_stg>(:start_dttm) AND ab_abcontact.updatetime_stg <= (:end_dttm)

    ) main_query )

    ab_abcontact 

 WHERE

/* ab_abcontact.TL_CNT_Name IN (''Person'',''Adjudicator'',''User Contact'',''Vendor (Person)'',''Attorney'', ''Doctor'',''Policy Person'') AND*/
     ((SOURCE = ''ClaimCenter'' AND PublicID IS NOT NULL) OR (SOURCE = ''ContactManager'' AND LinkID IS NOT NULL)) AND

      licensenumber IS NOT NULL AND 

      PRODUCER_CODE IS NULL 

/* AND   TaxID  IS NULL*/






UNION



/* Find the logic that is brining in wrong Prty_idntftn_num for GWID */
SELECT  DISTINCT

     ab_abcontact.partyidentificationnum AS partyidentificationnum,

     ab_abcontact.PublicID AS PublicID,

     ab_abcontact.LinkID AS LinkID,

     ''PRTY_IDNTFTN_TYPE13'' AS PRTY_IDNTFTN_TYPE_CD,

    SOURCE,

    ab_abcontact.SYS_SRC_CD AS SYS_SRC_CD,

   ab_abcontact.createtime,

   ab_abcontact.Retired AS Retired,

   ab_abcontact.TL_CNT_Name,

   case 

    when right(cast(extract(second from  prty_idntftn_updatetime ) as varchar(24)),4) between 1000 and 1499 then cast(cast( prty_idntftn_updatetime  as varchar(22))||''0000'' as timestamp(6)) 

    when right(cast(extract(second from prty_idntftn_updatetime ) as varchar(24)),4) between 1500 and 4499 then cast(cast( prty_idntftn_updatetime  as varchar(22))||''3000'' as timestamp(6))

    when right(cast(extract(second from  prty_idntftn_updatetime ) as varchar(24)),4) between 4500 and 8499 then cast(cast( prty_idntftn_updatetime  as varchar(22))||''7000'' as timestamp(6)) 

    when right(cast(extract(second from  prty_idntftn_updatetime ) as varchar(24)),4) between 8500 and 9999 then cast(cast( prty_idntftn_updatetime  as varchar(22))||''0000'' as timestamp(6)) + INTERVAL ''0.010 SECOND''

    else  prty_idntftn_updatetime 

    end as  prty_idntftn_updatetime 

FROM (



select main_query.TaxID,main_query.PublicID,main_query.LinkID,main_query.Retired,main_query.CreateTime,main_query.LicenseNumber,main_query.TL_CNT_Name,

main_query.Partyidentificationnum,main_query.HICN_alfa,main_query.AdjusterCode_alfa,main_query.SYS_SRC_CD,main_query.PRODUCER_CODE,main_query.SOURCE,main_query.prty_idntftn_updatetime  from (

SELECT  

    bc_contact.TaxID_stg as TaxID,

    bc_contact.PublicID_stg AS PublicID,

    bc_contact.AddressBookUID_stg AS LinkID,

    bc_contact.Retired_stg as Retired,

    bc_contact.CreateTime_stg as CreateTime,

    bc_contact.LicenseNumber_stg as LicenseNumber,

    bctl_contact.NAME_stg AS TL_CNT_Name,

    bc_credential.username_stg AS Partyidentificationnum,

    cast(null as varchar(255)) AS HICN_alfa,

    cast(null as varchar(255)) AS  AdjusterCode_alfa,

    ''SRC_SYS5'' AS SYS_SRC_CD,

    cast(null as varchar(255)) AS PRODUCER_CODE,

    cast(''ClaimCenter'' as varchar(255)) as SOURCE,

    case 

            when (COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))  and

            (COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

            then bc_contact.updatetime_stg

            when (COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE ))) and 

            (COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

            then bc_credential.updatetime_stg

            when (COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))    and 

            (COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

            then bc_user.updatetime_stg

    end as prty_idntftn_updatetime

FROM

    DB_T_PROD_STAG.bc_contact 

    LEFT OUTER JOIN DB_T_PROD_STAG.bctl_contact    ON bctl_contact.id_stg = bc_contact.subtype_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.bctl_gendertype ON bc_contact.gender_stg = bctl_gendertype.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.bctl_taxfilingstatustype ON bc_contact.TaxFilingStatus_stg = bctl_taxfilingstatustype.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.bctl_taxstatus  ON bc_contact.taxstatus_stg = bctl_taxstatus.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.bctl_maritalstatus  ON bc_contact.Maritalstatus_stg = bctl_maritalstatus.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.bctl_nameprefix ON bc_contact.prefix_stg = bctl_nameprefix.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.bctl_namesuffix ON bc_contact.Suffix_stg = bctl_namesuffix.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.bc_user ON bc_user.ContactID_stg = bc_contact.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.bc_credential ON bc_user.CredentialID_stg = bc_credential.id_stg

WHERE   bctl_contact.typecode_stg = (''UserContact'')  

    AND 

/*  below condition added to avoid duplicates*/
       bc_contact.PublicID_stg not in (''default_data:1'',

        ''systemTables:1'',''systemTables:2'') 

    and ((bc_contact.updatetime_stg>(:start_dttm) 

    AND bc_contact.updatetime_stg <=(:end_dttm)) 

    OR (bc_user.updatetime_stg>(:start_dttm) 

    AND bc_user.updatetime_stg <= (:end_dttm))

OR (bc_credential.updatetime_stg>(:start_dttm) /* add following OR condition where prty_idntftn_type_cd =13 for EIM-45249 */
	AND bc_credential.updatetime_stg<=(:end_dttm))

	)

	

        

UNION



SELECT  

    bc_contact.TaxID_stg as TaxID,

    case 

        when(bc_contact.ExternalID_stg is not null) then bc_contact.ExternalID_stg 

        else bc_contact.PublicID_stg 

        end AS PublicID,

    bc_contact.AddressBookUID_stg AS LinkID,

    bc_contact.Retired_stg as Retired,

    bc_contact.CreateTime_stg as CreateTime,

    bc_contact.LicenseNumber_stg as LicenseNumber,

    bctl_contact.NAME_stg AS TL_CNT_Name,

    bc_credential.username_stg AS Partyidentificationnum,

    cast(null as varchar(255)) AS HICN_alfa,

    cast(null as varchar(255)) AS  AdjusterCode_alfa,

    ''SRC_SYS5'' AS SYS_SRC_CD,

    cast(null as varchar(255)) AS PRODUCER_CODE, 

    cast(''ClaimCenter'' as varchar(255)) as SOURCE,

    case 

        when (COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))  and

        (COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then bc_contact.updatetime_stg

        when (COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )))     and 

        (COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then bc_credential.updatetime_stg

        when (COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))    and 

        (COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then bc_user.updatetime_stg

    end as prty_idntftn_updatetime 

from    DB_T_PROD_STAG.bc_account a

inner join DB_T_PROD_STAG.bc_accountcontact h  on h.AccountID_stg = a.id_stg

inner join DB_T_PROD_STAG.bc_contact   on bc_contact.id_stg = h.ContactID_stg

join DB_T_PROD_STAG.bctl_contact   on bctl_contact.id_stg=bc_contact.subtype_stg

left join DB_T_PROD_STAG.bc_accountcontactrole i   on i.AccountContactID_stg = h.id_stg

left join DB_T_PROD_STAG.bctl_accountrole j    on j.id_stg = i.Role_stg

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_gendertype     ON bc_contact.gender_stg = bctl_gendertype.id_stg 

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_taxfilingstatustype    ON bc_contact.TaxFilingStatus_stg = bctl_taxfilingstatustype.id_stg 

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_taxstatus  ON bc_contact.taxstatus_stg = bctl_taxstatus.id_stg

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_maritalstatus  ON bc_contact.Maritalstatus_stg = bctl_maritalstatus.id_stg 

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_nameprefix     ON bc_contact.prefix_stg = bctl_nameprefix.id_stg 

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_namesuffix     ON bc_contact.Suffix_stg = bctl_namesuffix.id_stg

LEFT OUTER JOIN DB_T_PROD_STAG.bc_user     ON bc_user.ContactID_stg = bc_contact.id_stg

LEFT OUTER JOIN DB_T_PROD_STAG.bc_credential   on bc_user.CredentialID_stg = bc_credential.id_stg

where   ((h.PrimaryPayer_stg = 1) 

    or (j.NAME_stg = ''Payer''))

    and ((bc_contact.updatetime_stg>(:start_dttm) 

    AND bc_contact.updatetime_stg <=(:end_dttm)) 

    OR (bc_user.updatetime_stg>(:start_dttm) 

    AND bc_user.updatetime_stg <= (:end_dttm))

OR (bc_credential.updatetime_stg>(:start_dttm) /* add following OR condition where prty_idntftn_type_cd =13 for EIM-45249 */
	AND bc_credential.updatetime_stg<=(:end_dttm))

	)



UNION

/*    Primary Payer and Overiding Payer Contact (this is at the Invoicestream level)*/
SELECT  

    bc_contact.TaxID_stg as TaxID,

    case 

        when (bc_contact.ExternalID_stg is null) then bc_contact.PublicID_stg 

        else bc_contact.ExternalID_stg 

        end AS PublicID,

    bc_contact.AddressBookUID_stg AS LinkID,

    bc_contact.Retired_stg as Retired,

    bc_contact.CreateTime_stg as CreateTime,

    bc_contact.LicenseNumber_stg as LicenseNumber,

    bctl_contact.NAME_stg AS TL_CNT_Name,

    bc_credential.username_stg AS Partyidentificationnum,

    cast(null as varchar(255)) AS HICN_alfa,

    cast(null as varchar(255)) AS  AdjusterCode_alfa,

    ''SRC_SYS5'' AS SYS_SRC_CD,

    cast(null as varchar(255)) AS PRODUCER_CODE,

    cast(''ClaimCenter'' as varchar(255)) as SOURCE,

    case 

        when (COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))  and

        (COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then bc_contact.updatetime_stg

        when (COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )))     and 

        (COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then bc_credential.updatetime_stg

        when (COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))    and 

        (COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then bc_user.updatetime_stg

    end as prty_idntftn_updatetime 

from    DB_T_PROD_STAG.bc_account a

inner join DB_T_PROD_STAG.bc_invoicestream b   on a.id_stg = b.AccountID_stg

inner join DB_T_PROD_STAG.bc_accountcontact c  on c.AccountID_stg=a.id_stg

inner join DB_T_PROD_STAG.bc_contact   on bc_contact.id_stg = c.ContactID_stg

join DB_T_PROD_STAG.bctl_contact   on bctl_contact.id_stg=bc_contact.subtype_stg

left join DB_T_PROD_STAG.bc_accountcontactrole f   on f.AccountContactID_stg = c.id_stg

left join DB_T_PROD_STAG.bctl_accountrole g    on g.id_stg = f.Role_stg

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_gendertype     ON bc_contact.gender_stg = bctl_gendertype.id_stg 

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_taxfilingstatustype    ON bc_contact.TaxFilingStatus_stg = bctl_taxfilingstatustype.id_stg 

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_taxstatus  ON bc_contact.taxstatus_stg = bctl_taxstatus.id_stg

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_maritalstatus  ON bc_contact.Maritalstatus_stg = bctl_maritalstatus.id_stg 

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_nameprefix     ON bc_contact.prefix_stg = bctl_nameprefix.id_stg 

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_namesuffix     ON bc_contact.Suffix_stg = bctl_namesuffix.id_stg

LEFT OUTER JOIN DB_T_PROD_STAG.bc_user     ON bc_user.ContactID_stg = bc_contact.id_stg

LEFT OUTER JOIN DB_T_PROD_STAG.bc_credential   on bc_user.CredentialID_stg = bc_credential.id_stg

where   ((b.OverridingPayer_alfa_stg is null 

    and c.PrimaryPayer_stg = 1) 

    or (b.OverridingPayer_alfa_stg is not null))

    and ((bc_contact.updatetime_stg>(:start_dttm) 

    AND bc_contact.updatetime_stg <=(:end_dttm)) 

    OR (bc_user.updatetime_stg>(:start_dttm) 

    AND bc_user.updatetime_stg <= (:end_dttm))

OR (bc_credential.updatetime_stg>(:start_dttm) /* add following OR condition where prty_idntftn_type_cd =13 for EIM-45249 */
	AND bc_credential.updatetime_stg<=(:end_dttm))

	)



UNION



SELECT  

    pc_contact.TaxID_stg as TaxID,

    pc_contact.PublicID_stg as PublicID,

    pc_contact.AddressBookUID_stg AS LinkID,

    pc_contact.Retired_stg as Retired,

    pc_contact.CreateTime_stg as CreateTime,

    pc_contact.LicenseNumber_stg as LicenseNumber,

    pctl_contact.NAME_stg AS TL_CNT_Name,

    pc_credential.username_stg AS Partyidentificationnum,

    cast(null as varchar(255)) AS HICN_alfa,

    cast(null as varchar(255)) as AdjusterCode_alfa,

/* pc_producercode.code as agentnumber, */
    ''SRC_SYS4'' AS SYS_SRC_CD,

    cast(null as varchar(255)) as PRODUCER_CODE,

    cast(''ClaimCenter'' as varchar(255)) as SOURCE,

    case 

        when (COALESCE(pc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(pc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))  and

        (COALESCE(pc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(pc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then pc_contact.updatetime_stg

        when (COALESCE(pc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(pc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )))     and 

        (COALESCE(pc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(pc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then pc_credential.updatetime_stg

        when (COALESCE(pc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(pc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))    and 

        (COALESCE(pc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(pc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then pc_user.updatetime_stg

    end as prty_idntftn_updatetime  

FROM

    DB_T_PROD_STAG.pc_contact 

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_contact    ON pctl_contact.id_stg = pc_contact.subtype_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_gendertype     ON pc_contact.gender_stg = pctl_gendertype.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_taxfilingstatustype    ON pc_contact.TaxFilingStatus_stg = pctl_taxfilingstatustype.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_taxstatus  ON pc_contact.taxstatus_stg = pctl_taxstatus.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_maritalstatus  ON pc_contact.Maritalstatus_stg = pctl_maritalstatus.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_nameprefix     ON pc_contact.prefix_stg = pctl_nameprefix.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_namesuffix     ON pc_contact.Suffix_stg = pctl_namesuffix.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.pc_user     ON pc_user.ContactID_stg = pc_contact.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.pc_credential   on pc_user.CredentialID_stg = pc_credential.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod     on pc_policyperiod.PNIContactDenorm_stg = pc_contact.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.pc_effectivedatedfields     on pc_effectivedatedfields.BranchID_stg = pc_policyperiod.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.pc_producercode     on pc_producercode.id_stg=pc_effectivedatedfields.ProducerCodeID_stg

WHERE

    pctl_contact.typecode_stg = (''UserContact'')  

    AND 

/*  below condition added to avoid duplicates*/
       pc_contact.PublicID_stg not in (''default_data:1'',

        ''systemTables:1'',''systemTables:2'') 

     AND ((pc_contact.updatetime_stg>(:start_dttm) 

    AND pc_contact.updatetime_stg <=(:end_dttm)) 

    OR (pc_user.updatetime_stg>(:start_dttm) 

    AND pc_user.updatetime_stg <= (:end_dttm))

OR (pc_credential.updatetime_stg>(:start_dttm) /* add following OR condition where prty_idntftn_type_cd =13 for EIM-45249 */
	AND pc_credential.updatetime_stg<=(:end_dttm))

	)

     

UNION



/* DB_T_PROD_STAG.CC_CONTACT */
SELECT  DISTINCT 

    cc_contact.TaxID_stg as TaxID,

    cc_contact.PublicID_stg as PublicID,

    cc_contact.AddressBookUID_stg AS LinkID,

    cc_contact.Retired_stg as Retired,

    cc_contact.CreateTime_stg as CreateTime,

    cc_contact.LicenseNumber_stg as LicenseNumber,

    cctl_contact.NAME_stg AS TL_CNT_Name,

    cc_credential.username_stg AS Partyidentificationnum,

/* cc_incident.HICN_alfa_stg AS HICN_alfa,*/
    cc_contact.HICN_alfa_stg AS HICN_alfa,

    cc_user.AdjusterCode_alfa_stg as AdjusterCode_alfa,

    ''SRC_SYS6'' AS SYS_SRC_CD,

    cast(null as varchar(255)) as PRODUCER_CODE,

    cast(''ClaimCenter'' as varchar(255)) as SOURCE,

    case 

        when (COALESCE(cc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(cc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))  and

        (COALESCE(cc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(cc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then cc_contact.updatetime_stg

        when (COALESCE(cc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(cc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )))     and 

        (COALESCE(cc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(cc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then cc_credential.updatetime_stg

        when (COALESCE(cc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(cc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))    and 

        (COALESCE(cc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(cc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then cc_user.updatetime_stg

    end as prty_idntftn_updatetime 



FROM

    DB_T_PROD_STAG.CC_CONTACT 

    LEFT OUTER JOIN DB_T_PROD_STAG.cctl_contact    ON cctl_contact.id_stg = cc_contact.subtype_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.cctl_gendertype     ON cc_contact.gender_stg = cctl_gendertype.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.cctl_taxfilingstatustype    ON cc_contact.TaxFilingStatus_stg = cctl_taxfilingstatustype.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.cctl_taxstatus  ON cc_contact.taxstatus_stg = cctl_taxstatus.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.cctl_maritalstatus  ON cc_contact.Maritalstatus_stg = cctl_maritalstatus.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.cctl_nameprefix     ON cc_contact.prefix_stg = cctl_nameprefix.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.cctl_namesuffix     ON cc_contact.Suffix_stg = cctl_namesuffix.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.cc_user     ON cc_user.ContactID_stg = cc_contact.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.cc_credential   ON cc_user.CredentialID_stg = cc_credential.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.cc_claimcontact ON cc_contact.id_stg=cc_claimcontact.ContactID_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.cc_claimcontactrole ON cc_claimcontact.id_stg=cc_claimcontactrole.claimcontactid_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.cc_incident     ON cc_claimcontactrole.claimcontactid_stg =cc_incident.id_stg

WHERE

    ((cc_contact.updatetime_stg>(:start_dttm) 

    AND cc_contact.updatetime_stg <=(:end_dttm)) 

    OR (cc_user.updatetime_stg>(:start_dttm) 

    AND cc_user.updatetime_stg <= (:end_dttm))

OR (cc_credential.updatetime_stg>(:start_dttm) /* add following OR condition where prty_idntftn_type_cd =13 for EIM-45249 */
	AND cc_credential.updatetime_stg<=(:end_dttm))

	)

      

UNION



SELECT  

    ab_abcontact.TaxID_stg as TaxID,

    cast(null as varchar(255)) AS PublicID,

    ab_abcontact.LinkID_stg as LinkID,  

    ab_abcontact.Retired_stg as Retired,

    ab_abcontact.CreateTime_stg as CreateTime,

    ab_abcontact.LicenseNumber_stg as LicenseNumber,

    abtl_abcontact.NAME_stg AS TL_CNT_Name,

    ab_credential.username_stg AS Partyidentificationnum,

    cast(null as varchar(255)) AS HICN_alfa,

    cast(null as varchar(255)) as AdjusterCode_alfa, 

/* abtl_occupation.typecode_stg,*/
    ''SRC_SYS7'' AS SYS_SRC_CD,

    LKP_Agent_Code.Code as PRODUCER_CODE,

    cast(''ContactManager'' as varchar(255)) as SOURCE,

    case 

        when (COALESCE(ab_abcontact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(ab_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))    and

        (COALESCE(ab_abcontact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(ab_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then ab_abcontact.updatetime_stg

        when (COALESCE(ab_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(ab_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )))     and 

        (COALESCE(ab_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(ab_abcontact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then ab_credential.updatetime_stg

        when (COALESCE(ab_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(ab_abcontact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))  and 

        (COALESCE(ab_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(ab_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then ab_user.updatetime_stg

    end as prty_idntftn_updatetime 

FROM

    DB_T_PROD_STAG.ab_abcontact

    LEFT OUTER JOIN DB_T_PROD_STAG.abtl_abcontact  ON abtl_abcontact.id_stg = ab_abcontact.subtype_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.abtl_gendertype     ON ab_abcontact.gender_stg = abtl_gendertype.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.abtl_taxfilingstatustype    ON ab_abcontact.TaxFilingStatus_stg = abtl_taxfilingstatustype.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.abtl_taxstatus  ON ab_abcontact.taxstatus_stg = abtl_taxstatus.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.abtl_maritalstatus  ON ab_abcontact.Maritalstatus_stg = abtl_maritalstatus.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.abtl_nameprefix     ON ab_abcontact.prefix_stg = abtl_nameprefix.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.abtl_namesuffix     ON ab_abcontact.Suffix_stg = abtl_namesuffix.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.ab_user     ON ab_user.ContactID_stg = ab_abcontact.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.ab_credential   on ab_user.CredentialID_stg = ab_credential.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.abtl_occupation     ON  abtl_occupation.id_stg = ab_abcontact.occupation_alfa_stg

    LEFT OUTER JOIN (   select   contact.AddressBookUID_stg AS AddressBookUID,  producer_cd.Code_stg AS Code  

    from     DB_T_PROD_STAG.pc_producercode producer_cd    

    join DB_T_PROD_STAG.pc_userproducercode user_producer      on (user_producer.ProducerCodeID_stg = producer_cd.id_stg)

    join DB_T_PROD_STAG.pc_user  user1         on(user1.id_stg= user_producer.UserID_stg)

    join DB_T_PROD_STAG.pc_contact contact         on(contact.id_stg = user1.ContactID_stg )    ) AS LKP_Agent_Code 

    on LKP_Agent_Code.AddressBookUID = ab_abcontact.LinkID_stg

    WHERE ab_abcontact.updatetime_stg>(:start_dttm) AND ab_abcontact.updatetime_stg <= (:end_dttm)

    ) main_query )

    ab_abcontact 

 WHERE

/* ab_abcontact.TL_CNT_Name IN (''Person'',''Adjudicator'',''User Contact'',''Vendor (Person)'',''Attorney'', ''Doctor'',''Policy Person'') AND*/
     ((SOURCE = ''ClaimCenter'' AND PublicID IS NOT NULL) OR (SOURCE = ''ContactManager'' AND LinkID IS NOT NULL)) AND

    ab_abcontact.partyidentificationnum IS NOT NULL



/*  this union ends here */


UNION



SELECT  DISTINCT

     ab_abcontact.HICN_alfa AS HICN_alfa,

     ab_abcontact.PublicID AS PublicID,

     ab_abcontact.LinkID AS LinkID,

     ''PRTY_IDNTFTN_TYPE8'' AS PRTY_IDNTFTN_TYPE_CD,

    SOURCE,

    ab_abcontact.SYS_SRC_CD AS SYS_SRC_CD,

ab_abcontact.createtime,

ab_abcontact.Retired AS Retired,

ab_abcontact.TL_CNT_Name,

  case 

    when right(cast(extract(second from  prty_idntftn_updatetime ) as varchar(24)),4) between 1000 and 1499 then cast(cast( prty_idntftn_updatetime  as varchar(22))||''0000'' as timestamp(6)) 

    when right(cast(extract(second from prty_idntftn_updatetime ) as varchar(24)),4) between 1500 and 4499 then cast(cast( prty_idntftn_updatetime  as varchar(22))||''3000'' as timestamp(6))

    when right(cast(extract(second from  prty_idntftn_updatetime ) as varchar(24)),4) between 4500 and 8499 then cast(cast( prty_idntftn_updatetime  as varchar(22))||''7000'' as timestamp(6)) 

    when right(cast(extract(second from  prty_idntftn_updatetime ) as varchar(24)),4) between 8500 and 9999 then cast(cast( prty_idntftn_updatetime  as varchar(22))||''0000'' as timestamp(6)) + INTERVAL ''0.010 SECOND''

    else  prty_idntftn_updatetime 

    end as  prty_idntftn_updatetime

FROM (





select main_query.TaxID,main_query.PublicID,main_query.LinkID,main_query.Retired,main_query.CreateTime,main_query.LicenseNumber,main_query.TL_CNT_Name,

main_query.Partyidentificationnum,main_query.HICN_alfa,main_query.AdjusterCode_alfa,main_query.SYS_SRC_CD,main_query.PRODUCER_CODE,main_query.SOURCE,main_query.prty_idntftn_updatetime  from (

SELECT  

    bc_contact.TaxID_stg as TaxID,

    bc_contact.PublicID_stg AS PublicID,

    bc_contact.AddressBookUID_stg AS LinkID,

    bc_contact.Retired_stg as Retired,

    bc_contact.CreateTime_stg as CreateTime,

    bc_contact.LicenseNumber_stg as LicenseNumber,

    bctl_contact.NAME_stg AS TL_CNT_Name,

    bc_credential.username_stg AS Partyidentificationnum,

    cast(null as varchar(255)) AS HICN_alfa,

    cast(null as varchar(255)) AS  AdjusterCode_alfa,

    ''SRC_SYS5'' AS SYS_SRC_CD,

    cast(null as varchar(255)) AS PRODUCER_CODE,

    cast(''ClaimCenter'' as varchar(255)) as SOURCE,

    case 

            when (COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))  and

            (COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

            then bc_contact.updatetime_stg

            when (COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE ))) and 

            (COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

            then bc_credential.updatetime_stg

            when (COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))    and 

            (COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

            then bc_user.updatetime_stg

    end as prty_idntftn_updatetime

FROM

    DB_T_PROD_STAG.bc_contact 

    LEFT OUTER JOIN DB_T_PROD_STAG.bctl_contact    ON bctl_contact.id_stg = bc_contact.subtype_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.bctl_gendertype ON bc_contact.gender_stg = bctl_gendertype.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.bctl_taxfilingstatustype ON bc_contact.TaxFilingStatus_stg = bctl_taxfilingstatustype.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.bctl_taxstatus  ON bc_contact.taxstatus_stg = bctl_taxstatus.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.bctl_maritalstatus  ON bc_contact.Maritalstatus_stg = bctl_maritalstatus.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.bctl_nameprefix ON bc_contact.prefix_stg = bctl_nameprefix.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.bctl_namesuffix ON bc_contact.Suffix_stg = bctl_namesuffix.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.bc_user ON bc_user.ContactID_stg = bc_contact.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.bc_credential ON bc_user.CredentialID_stg = bc_credential.id_stg

WHERE   bctl_contact.typecode_stg = (''UserContact'')  

    AND 

/*  below condition added to avoid duplicates*/
       bc_contact.PublicID_stg not in (''default_data:1'',

        ''systemTables:1'',''systemTables:2'') 

    and ((bc_contact.updatetime_stg>(:start_dttm) 

    AND bc_contact.updatetime_stg <=(:end_dttm)) 

    OR (bc_user.updatetime_stg>(:start_dttm) 

    AND bc_user.updatetime_stg <= (:end_dttm)))

        

UNION



SELECT  

    bc_contact.TaxID_stg as TaxID,

    case 

        when(bc_contact.ExternalID_stg is not null) then bc_contact.ExternalID_stg 

        else bc_contact.PublicID_stg 

        end AS PublicID,

    bc_contact.AddressBookUID_stg AS LinkID,

    bc_contact.Retired_stg as Retired,

    bc_contact.CreateTime_stg as CreateTime,

    bc_contact.LicenseNumber_stg as LicenseNumber,

    bctl_contact.NAME_stg AS TL_CNT_Name,

    bc_credential.username_stg AS Partyidentificationnum,

    cast(null as varchar(255)) AS HICN_alfa,

    cast(null as varchar(255)) AS  AdjusterCode_alfa,

    ''SRC_SYS5'' AS SYS_SRC_CD,

    cast(null as varchar(255)) AS PRODUCER_CODE, 

    cast(''ClaimCenter'' as varchar(255)) as SOURCE,

    case 

        when (COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))  and

        (COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then bc_contact.updatetime_stg

        when (COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )))     and 

        (COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then bc_credential.updatetime_stg

        when (COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))    and 

        (COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then bc_user.updatetime_stg

    end as prty_idntftn_updatetime 

from    DB_T_PROD_STAG.bc_account a

inner join DB_T_PROD_STAG.bc_accountcontact h  on h.AccountID_stg = a.id_stg

inner join DB_T_PROD_STAG.bc_contact   on bc_contact.id_stg = h.ContactID_stg

join DB_T_PROD_STAG.bctl_contact   on bctl_contact.id_stg=bc_contact.subtype_stg

left join DB_T_PROD_STAG.bc_accountcontactrole i   on i.AccountContactID_stg = h.id_stg

left join DB_T_PROD_STAG.bctl_accountrole j    on j.id_stg = i.Role_stg

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_gendertype     ON bc_contact.gender_stg = bctl_gendertype.id_stg 

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_taxfilingstatustype    ON bc_contact.TaxFilingStatus_stg = bctl_taxfilingstatustype.id_stg 

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_taxstatus  ON bc_contact.taxstatus_stg = bctl_taxstatus.id_stg

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_maritalstatus  ON bc_contact.Maritalstatus_stg = bctl_maritalstatus.id_stg 

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_nameprefix     ON bc_contact.prefix_stg = bctl_nameprefix.id_stg 

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_namesuffix     ON bc_contact.Suffix_stg = bctl_namesuffix.id_stg

LEFT OUTER JOIN DB_T_PROD_STAG.bc_user     ON bc_user.ContactID_stg = bc_contact.id_stg

LEFT OUTER JOIN DB_T_PROD_STAG.bc_credential   on bc_user.CredentialID_stg = bc_credential.id_stg

where   ((h.PrimaryPayer_stg = 1) 

    or (j.NAME_stg = ''Payer''))

    and ((bc_contact.updatetime_stg>(:start_dttm) 

    AND bc_contact.updatetime_stg <=(:end_dttm)) 

    OR (bc_user.updatetime_stg>(:start_dttm) 

    AND bc_user.updatetime_stg <= (:end_dttm)))



UNION

/*    Primary Payer and Overiding Payer Contact (this is at the Invoicestream level)*/
SELECT  

    bc_contact.TaxID_stg as TaxID,

    case 

        when (bc_contact.ExternalID_stg is null) then bc_contact.PublicID_stg 

        else bc_contact.ExternalID_stg 

        end AS PublicID,

    bc_contact.AddressBookUID_stg AS LinkID,

    bc_contact.Retired_stg as Retired,

    bc_contact.CreateTime_stg as CreateTime,

    bc_contact.LicenseNumber_stg as LicenseNumber,

    bctl_contact.NAME_stg AS TL_CNT_Name,

    bc_credential.username_stg AS Partyidentificationnum,

    cast(null as varchar(255)) AS HICN_alfa,

    cast(null as varchar(255)) AS  AdjusterCode_alfa,

    ''SRC_SYS5'' AS SYS_SRC_CD,

    cast(null as varchar(255)) AS PRODUCER_CODE,

    cast(''ClaimCenter'' as varchar(255)) as SOURCE,

    case 

        when (COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))  and

        (COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then bc_contact.updatetime_stg

        when (COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )))     and 

        (COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then bc_credential.updatetime_stg

        when (COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))    and 

        (COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then bc_user.updatetime_stg

    end as prty_idntftn_updatetime 

from    DB_T_PROD_STAG.bc_account a

inner join DB_T_PROD_STAG.bc_invoicestream b   on a.id_stg = b.AccountID_stg

inner join DB_T_PROD_STAG.bc_accountcontact c  on c.AccountID_stg=a.id_stg

inner join DB_T_PROD_STAG.bc_contact   on bc_contact.id_stg = c.ContactID_stg

join DB_T_PROD_STAG.bctl_contact   on bctl_contact.id_stg=bc_contact.subtype_stg

left join DB_T_PROD_STAG.bc_accountcontactrole f   on f.AccountContactID_stg = c.id_stg

left join DB_T_PROD_STAG.bctl_accountrole g    on g.id_stg = f.Role_stg

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_gendertype     ON bc_contact.gender_stg = bctl_gendertype.id_stg 

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_taxfilingstatustype    ON bc_contact.TaxFilingStatus_stg = bctl_taxfilingstatustype.id_stg 

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_taxstatus  ON bc_contact.taxstatus_stg = bctl_taxstatus.id_stg

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_maritalstatus  ON bc_contact.Maritalstatus_stg = bctl_maritalstatus.id_stg 

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_nameprefix     ON bc_contact.prefix_stg = bctl_nameprefix.id_stg 

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_namesuffix     ON bc_contact.Suffix_stg = bctl_namesuffix.id_stg

LEFT OUTER JOIN DB_T_PROD_STAG.bc_user     ON bc_user.ContactID_stg = bc_contact.id_stg

LEFT OUTER JOIN DB_T_PROD_STAG.bc_credential   on bc_user.CredentialID_stg = bc_credential.id_stg

where   ((b.OverridingPayer_alfa_stg is null 

    and c.PrimaryPayer_stg = 1) 

    or (b.OverridingPayer_alfa_stg is not null))

    and ((bc_contact.updatetime_stg>(:start_dttm) 

    AND bc_contact.updatetime_stg <=(:end_dttm)) 

    OR (bc_user.updatetime_stg>(:start_dttm) 

    AND bc_user.updatetime_stg <= (:end_dttm)))



UNION



SELECT  

    pc_contact.TaxID_stg as TaxID,

    pc_contact.PublicID_stg as PublicID,

    pc_contact.AddressBookUID_stg AS LinkID,

    pc_contact.Retired_stg as Retired,

    pc_contact.CreateTime_stg as CreateTime,

    pc_contact.LicenseNumber_stg as LicenseNumber,

    pctl_contact.NAME_stg AS TL_CNT_Name,

    pc_credential.username_stg AS Partyidentificationnum,

    cast(null as varchar(255)) AS HICN_alfa,

    cast(null as varchar(255)) as AdjusterCode_alfa,

/* pc_producercode.code as agentnumber, */
    ''SRC_SYS4'' AS SYS_SRC_CD,

    cast(null as varchar(255)) as PRODUCER_CODE,

    cast(''ClaimCenter'' as varchar(255)) as SOURCE,

    case 

        when (COALESCE(pc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(pc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))  and

        (COALESCE(pc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(pc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then pc_contact.updatetime_stg

        when (COALESCE(pc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(pc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )))     and 

        (COALESCE(pc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(pc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then pc_credential.updatetime_stg

        when (COALESCE(pc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(pc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))    and 

        (COALESCE(pc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(pc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then pc_user.updatetime_stg

    end as prty_idntftn_updatetime  

FROM

    DB_T_PROD_STAG.pc_contact 

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_contact    ON pctl_contact.id_stg = pc_contact.subtype_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_gendertype     ON pc_contact.gender_stg = pctl_gendertype.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_taxfilingstatustype    ON pc_contact.TaxFilingStatus_stg = pctl_taxfilingstatustype.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_taxstatus  ON pc_contact.taxstatus_stg = pctl_taxstatus.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_maritalstatus  ON pc_contact.Maritalstatus_stg = pctl_maritalstatus.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_nameprefix     ON pc_contact.prefix_stg = pctl_nameprefix.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_namesuffix     ON pc_contact.Suffix_stg = pctl_namesuffix.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.pc_user     ON pc_user.ContactID_stg = pc_contact.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.pc_credential   on pc_user.CredentialID_stg = pc_credential.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod     on pc_policyperiod.PNIContactDenorm_stg = pc_contact.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.pc_effectivedatedfields     on pc_effectivedatedfields.BranchID_stg = pc_policyperiod.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.pc_producercode     on pc_producercode.id_stg=pc_effectivedatedfields.ProducerCodeID_stg

WHERE

    pctl_contact.typecode_stg = (''UserContact'')  

    AND 

/*  below condition added to avoid duplicates*/
       pc_contact.PublicID_stg not in (''default_data:1'',

        ''systemTables:1'',''systemTables:2'') 

     AND ((pc_contact.updatetime_stg>(:start_dttm) 

    AND pc_contact.updatetime_stg <= (:end_dttm)) 

    OR (pc_user.updatetime_stg>(:start_dttm) 

    AND pc_user.updatetime_stg <= (:end_dttm)))

     

UNION



/* DB_T_PROD_STAG.CC_CONTACT */
SELECT  DISTINCT 

    cc_contact.TaxID_stg as TaxID,

    cc_contact.PublicID_stg as PublicID,

    cc_contact.AddressBookUID_stg AS LinkID,

    cc_contact.Retired_stg as Retired,

    cc_contact.CreateTime_stg as CreateTime,

    cc_contact.LicenseNumber_stg as LicenseNumber,

    cctl_contact.NAME_stg AS TL_CNT_Name,

    cc_credential.username_stg AS Partyidentificationnum,

/* cc_incident.HICN_alfa_stg AS HICN_alfa,*/
    cc_contact.HICN_alfa_stg AS HICN_alfa,

    cc_user.AdjusterCode_alfa_stg as AdjusterCode_alfa,

    ''SRC_SYS6'' AS SYS_SRC_CD,

    cast(null as varchar(255)) as PRODUCER_CODE,

    cast(''ClaimCenter'' as varchar(255)) as SOURCE,

    case 

        when (COALESCE(cc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(cc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))  and

        (COALESCE(cc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(cc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then cc_contact.updatetime_stg

        when (COALESCE(cc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(cc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )))     and 

        (COALESCE(cc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(cc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then cc_credential.updatetime_stg

        when (COALESCE(cc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(cc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))    and 

        (COALESCE(cc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(cc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then cc_user.updatetime_stg

    end as prty_idntftn_updatetime 



FROM

    DB_T_PROD_STAG.CC_CONTACT 

    LEFT OUTER JOIN DB_T_PROD_STAG.cctl_contact    ON cctl_contact.id_stg = cc_contact.subtype_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.cctl_gendertype     ON cc_contact.gender_stg = cctl_gendertype.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.cctl_taxfilingstatustype    ON cc_contact.TaxFilingStatus_stg = cctl_taxfilingstatustype.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.cctl_taxstatus  ON cc_contact.taxstatus_stg = cctl_taxstatus.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.cctl_maritalstatus  ON cc_contact.Maritalstatus_stg = cctl_maritalstatus.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.cctl_nameprefix     ON cc_contact.prefix_stg = cctl_nameprefix.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.cctl_namesuffix     ON cc_contact.Suffix_stg = cctl_namesuffix.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.cc_user     ON cc_user.ContactID_stg = cc_contact.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.cc_credential   ON cc_user.CredentialID_stg = cc_credential.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.cc_claimcontact ON cc_contact.id_stg=cc_claimcontact.ContactID_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.cc_claimcontactrole ON cc_claimcontact.id_stg=cc_claimcontactrole.claimcontactid_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.cc_incident     ON cc_claimcontactrole.claimcontactid_stg =cc_incident.id_stg

WHERE

    (cc_contact.updatetime_stg>(:start_dttm) 

    AND cc_contact.updatetime_stg <= (:end_dttm) )  

    or 

       (cc_user.updatetime_stg>(:start_dttm) 

    AND cc_user.updatetime_stg <=(:end_dttm) )

      

UNION



SELECT  

    ab_abcontact.TaxID_stg as TaxID,

    cast(null as varchar(255)) AS PublicID,

    ab_abcontact.LinkID_stg as LinkID,  

    ab_abcontact.Retired_stg as Retired,

    ab_abcontact.CreateTime_stg as CreateTime,

    ab_abcontact.LicenseNumber_stg as LicenseNumber,

    abtl_abcontact.NAME_stg AS TL_CNT_Name,

    ab_credential.username_stg AS Partyidentificationnum,

    cast(null as varchar(255)) AS HICN_alfa,

    cast(null as varchar(255)) as AdjusterCode_alfa, 

/* abtl_occupation.typecode_stg,*/
    ''SRC_SYS7'' AS SYS_SRC_CD,

    LKP_Agent_Code.Code as PRODUCER_CODE,

    cast(''ContactManager'' as varchar(255)) as SOURCE,

    case 

        when (COALESCE(ab_abcontact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(ab_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))    and

        (COALESCE(ab_abcontact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(ab_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then ab_abcontact.updatetime_stg

        when (COALESCE(ab_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(ab_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )))     and 

        (COALESCE(ab_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(ab_abcontact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then ab_credential.updatetime_stg

        when (COALESCE(ab_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(ab_abcontact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))  and 

        (COALESCE(ab_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(ab_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then ab_user.updatetime_stg

    end as prty_idntftn_updatetime 

FROM

    DB_T_PROD_STAG.ab_abcontact

    LEFT OUTER JOIN DB_T_PROD_STAG.abtl_abcontact  ON abtl_abcontact.id_stg = ab_abcontact.subtype_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.abtl_gendertype     ON ab_abcontact.gender_stg = abtl_gendertype.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.abtl_taxfilingstatustype    ON ab_abcontact.TaxFilingStatus_stg = abtl_taxfilingstatustype.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.abtl_taxstatus  ON ab_abcontact.taxstatus_stg = abtl_taxstatus.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.abtl_maritalstatus  ON ab_abcontact.Maritalstatus_stg = abtl_maritalstatus.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.abtl_nameprefix     ON ab_abcontact.prefix_stg = abtl_nameprefix.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.abtl_namesuffix     ON ab_abcontact.Suffix_stg = abtl_namesuffix.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.ab_user     ON ab_user.ContactID_stg = ab_abcontact.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.ab_credential   on ab_user.CredentialID_stg = ab_credential.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.abtl_occupation     ON  abtl_occupation.id_stg = ab_abcontact.occupation_alfa_stg

    LEFT OUTER JOIN (   select   contact.AddressBookUID_stg AS AddressBookUID,  producer_cd.Code_stg AS Code  

    from     DB_T_PROD_STAG.pc_producercode producer_cd    

    join DB_T_PROD_STAG.pc_userproducercode user_producer      on (user_producer.ProducerCodeID_stg = producer_cd.id_stg)

    join DB_T_PROD_STAG.pc_user  user1         on(user1.id_stg= user_producer.UserID_stg)

    join DB_T_PROD_STAG.pc_contact contact         on(contact.id_stg = user1.ContactID_stg )    ) AS LKP_Agent_Code 

    on LKP_Agent_Code.AddressBookUID = ab_abcontact.LinkID_stg

    WHERE ab_abcontact.updatetime_stg>(:start_dttm) AND ab_abcontact.updatetime_stg <= (:end_dttm)

    ) main_query )

    ab_abcontact 

 WHERE

/* ab_abcontact.TL_CNT_Name IN (''Person'',''Adjudicator'',''User Contact'',''Vendor (Person)'',''Attorney'', ''Doctor'',''Policy Person'') AND*/
     ((SOURCE = ''ClaimCenter'' AND PublicID IS NOT NULL) OR (SOURCE = ''ContactManager'' AND LinkID IS NOT NULL))

    AND ab_abcontact.HICN_alfa IS NOT NULL

    





UNION



/*  DB_T_CORE_PROD.Adjuster District  /* CLM_DA_0226 */
SELECT  DISTINCT

     ab_abcontact.AdjusterCode_alfa AS AdjusterCode_alfa,

     ab_abcontact.PublicID AS PublicID,

     ab_abcontact.LinkID AS LinkID,

     ''PRTY_IDNTFTN_TYPE14'' AS PRTY_IDNTFTN_TYPE_CD,

    SOURCE,

    ab_abcontact.SYS_SRC_CD AS SYS_SRC_CD,

ab_abcontact.createtime,

ab_abcontact.Retired AS Retired,

ab_abcontact.TL_CNT_Name, 
    CASE
  WHEN RIGHT (
    CAST(
      DATE_PART (SECOND, prty_idntftn_updatetime) AS VARCHAR(24)
    ),
    4
  ) BETWEEN ''1000''
  AND ''1499'' THEN TO_TIMESTAMP (
    LEFT (CAST(prty_idntftn_updatetime AS VARCHAR), 22) || ''0000''
  )
  WHEN RIGHT (
    CAST(
      DATE_PART (SECOND, prty_idntftn_updatetime) AS VARCHAR(24)
    ),
    4
  ) BETWEEN ''1500''
  AND ''4499'' THEN TO_TIMESTAMP (
    LEFT (CAST(prty_idntftn_updatetime AS VARCHAR), 22) || ''3000''
  )
  WHEN RIGHT (
    CAST(
      DATE_PART (SECOND, prty_idntftn_updatetime) AS VARCHAR(24)
    ),
    4
  ) BETWEEN ''4500''
  AND ''8499'' THEN TO_TIMESTAMP (
    LEFT (CAST(prty_idntftn_updatetime AS VARCHAR), 22) || ''7000''
  )
  WHEN RIGHT (
    CAST(
      DATE_PART (SECOND, prty_idntftn_updatetime) AS VARCHAR(24)
    ),
    4
  ) BETWEEN ''8500''
  AND ''9999'' THEN DATEADD (
    MILLISECOND,
    10,
    TO_TIMESTAMP (
      LEFT (CAST(prty_idntftn_updatetime AS VARCHAR), 22) || ''0000''
    )
  )
  ELSE prty_idntftn_updatetime
END as prty_idntftn_updatetime
FROM (



select main_query.TaxID,main_query.PublicID,main_query.LinkID,main_query.Retired,main_query.CreateTime,main_query.LicenseNumber,main_query.TL_CNT_Name,

main_query.Partyidentificationnum,main_query.HICN_alfa,main_query.AdjusterCode_alfa,main_query.SYS_SRC_CD,main_query.PRODUCER_CODE,main_query.SOURCE,main_query.prty_idntftn_updatetime  from (

SELECT  

    bc_contact.TaxID_stg as TaxID,

    bc_contact.PublicID_stg AS PublicID,

    bc_contact.AddressBookUID_stg AS LinkID,

    bc_contact.Retired_stg as Retired,

    bc_contact.CreateTime_stg as CreateTime,

    bc_contact.LicenseNumber_stg as LicenseNumber,

    bctl_contact.NAME_stg AS TL_CNT_Name,

    bc_credential.username_stg AS Partyidentificationnum,

    cast(null as varchar(255)) AS HICN_alfa,

    cast(null as varchar(255)) AS  AdjusterCode_alfa,

    ''SRC_SYS5'' AS SYS_SRC_CD,

    cast(null as varchar(255)) AS PRODUCER_CODE,

    cast(''ClaimCenter'' as varchar(255)) as SOURCE,

    case 

            when (COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))  and

            (COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

            then bc_contact.updatetime_stg

            when (COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE ))) and 

            (COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

            then bc_credential.updatetime_stg

            when (COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))    and 

            (COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

            then bc_user.updatetime_stg

    end as prty_idntftn_updatetime

FROM

    DB_T_PROD_STAG.bc_contact 

    LEFT OUTER JOIN DB_T_PROD_STAG.bctl_contact    ON bctl_contact.id_stg = bc_contact.subtype_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.bctl_gendertype ON bc_contact.gender_stg = bctl_gendertype.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.bctl_taxfilingstatustype ON bc_contact.TaxFilingStatus_stg = bctl_taxfilingstatustype.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.bctl_taxstatus  ON bc_contact.taxstatus_stg = bctl_taxstatus.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.bctl_maritalstatus  ON bc_contact.Maritalstatus_stg = bctl_maritalstatus.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.bctl_nameprefix ON bc_contact.prefix_stg = bctl_nameprefix.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.bctl_namesuffix ON bc_contact.Suffix_stg = bctl_namesuffix.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.bc_user ON bc_user.ContactID_stg = bc_contact.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.bc_credential ON bc_user.CredentialID_stg = bc_credential.id_stg

WHERE   bctl_contact.typecode_stg = (''UserContact'')  

    AND 

/*  below condition added to avoid duplicates*/
       bc_contact.PublicID_stg not in (''default_data:1'',

        ''systemTables:1'',''systemTables:2'') 

    and ((bc_contact.updatetime_stg>(:start_dttm) 

    AND bc_contact.updatetime_stg <=(:end_dttm)) 

    OR (bc_user.updatetime_stg>(:start_dttm) 

    AND bc_user.updatetime_stg <= (:end_dttm)))

        

UNION



SELECT  

    bc_contact.TaxID_stg as TaxID,

    case 

        when(bc_contact.ExternalID_stg is not null) then bc_contact.ExternalID_stg 

        else bc_contact.PublicID_stg 

        end AS PublicID,

    bc_contact.AddressBookUID_stg AS LinkID,

    bc_contact.Retired_stg as Retired,

    bc_contact.CreateTime_stg as CreateTime,

    bc_contact.LicenseNumber_stg as LicenseNumber,

    bctl_contact.NAME_stg AS TL_CNT_Name,

    bc_credential.username_stg AS Partyidentificationnum,

    cast(null as varchar(255)) AS HICN_alfa,

    cast(null as varchar(255)) AS  AdjusterCode_alfa,

    ''SRC_SYS5'' AS SYS_SRC_CD,

    cast(null as varchar(255)) AS PRODUCER_CODE, 

    cast(''ClaimCenter'' as varchar(255)) as SOURCE,

    case 

        when (COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))  and

        (COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then bc_contact.updatetime_stg

        when (COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )))     and 

        (COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then bc_credential.updatetime_stg

        when (COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))    and 

        (COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then bc_user.updatetime_stg

    end as prty_idntftn_updatetime 

from    DB_T_PROD_STAG.bc_account a

inner join DB_T_PROD_STAG.bc_accountcontact h  on h.AccountID_stg = a.id_stg

inner join DB_T_PROD_STAG.bc_contact   on bc_contact.id_stg = h.ContactID_stg

join DB_T_PROD_STAG.bctl_contact   on bctl_contact.id_stg=bc_contact.subtype_stg

left join DB_T_PROD_STAG.bc_accountcontactrole i   on i.AccountContactID_stg = h.id_stg

left join DB_T_PROD_STAG.bctl_accountrole j    on j.id_stg = i.Role_stg

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_gendertype     ON bc_contact.gender_stg = bctl_gendertype.id_stg 

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_taxfilingstatustype    ON bc_contact.TaxFilingStatus_stg = bctl_taxfilingstatustype.id_stg 

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_taxstatus  ON bc_contact.taxstatus_stg = bctl_taxstatus.id_stg

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_maritalstatus  ON bc_contact.Maritalstatus_stg = bctl_maritalstatus.id_stg 

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_nameprefix     ON bc_contact.prefix_stg = bctl_nameprefix.id_stg 

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_namesuffix     ON bc_contact.Suffix_stg = bctl_namesuffix.id_stg

LEFT OUTER JOIN DB_T_PROD_STAG.bc_user     ON bc_user.ContactID_stg = bc_contact.id_stg

LEFT OUTER JOIN DB_T_PROD_STAG.bc_credential   on bc_user.CredentialID_stg = bc_credential.id_stg

where   ((h.PrimaryPayer_stg = 1) 

    or (j.NAME_stg = ''Payer''))

    and ((bc_contact.updatetime_stg>(:start_dttm) 

    AND bc_contact.updatetime_stg <=(:end_dttm)) 

    OR (bc_user.updatetime_stg>(:start_dttm) 

    AND bc_user.updatetime_stg <= (:end_dttm)))



UNION

/*    Primary Payer and Overiding Payer Contact (this is at the Invoicestream level)*/
SELECT  

    bc_contact.TaxID_stg as TaxID,

    case 

        when (bc_contact.ExternalID_stg is null) then bc_contact.PublicID_stg 

        else bc_contact.ExternalID_stg 

        end AS PublicID,

    bc_contact.AddressBookUID_stg AS LinkID,

    bc_contact.Retired_stg as Retired,

    bc_contact.CreateTime_stg as CreateTime,

    bc_contact.LicenseNumber_stg as LicenseNumber,

    bctl_contact.NAME_stg AS TL_CNT_Name,

    bc_credential.username_stg AS Partyidentificationnum,

    cast(null as varchar(255)) AS HICN_alfa,

    cast(null as varchar(255)) AS  AdjusterCode_alfa,

    ''SRC_SYS5'' AS SYS_SRC_CD,

    cast(null as varchar(255)) AS PRODUCER_CODE,

    cast(''ClaimCenter'' as varchar(255)) as SOURCE,

    case 

        when (COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))  and

        (COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then bc_contact.updatetime_stg

        when (COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )))     and 

        (COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then bc_credential.updatetime_stg

        when (COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))    and 

        (COALESCE(bc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(bc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then bc_user.updatetime_stg

    end as prty_idntftn_updatetime 

from    DB_T_PROD_STAG.bc_account a

inner join DB_T_PROD_STAG.bc_invoicestream b   on a.id_stg = b.AccountID_stg

inner join DB_T_PROD_STAG.bc_accountcontact c  on c.AccountID_stg=a.id_stg

inner join DB_T_PROD_STAG.bc_contact   on bc_contact.id_stg = c.ContactID_stg

join DB_T_PROD_STAG.bctl_contact   on bctl_contact.id_stg=bc_contact.subtype_stg

left join DB_T_PROD_STAG.bc_accountcontactrole f   on f.AccountContactID_stg = c.id_stg

left join DB_T_PROD_STAG.bctl_accountrole g    on g.id_stg = f.Role_stg

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_gendertype     ON bc_contact.gender_stg = bctl_gendertype.id_stg 

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_taxfilingstatustype    ON bc_contact.TaxFilingStatus_stg = bctl_taxfilingstatustype.id_stg 

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_taxstatus  ON bc_contact.taxstatus_stg = bctl_taxstatus.id_stg

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_maritalstatus  ON bc_contact.Maritalstatus_stg = bctl_maritalstatus.id_stg 

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_nameprefix     ON bc_contact.prefix_stg = bctl_nameprefix.id_stg 

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_namesuffix     ON bc_contact.Suffix_stg = bctl_namesuffix.id_stg

LEFT OUTER JOIN DB_T_PROD_STAG.bc_user     ON bc_user.ContactID_stg = bc_contact.id_stg

LEFT OUTER JOIN DB_T_PROD_STAG.bc_credential   on bc_user.CredentialID_stg = bc_credential.id_stg

where   ((b.OverridingPayer_alfa_stg is null 

    and c.PrimaryPayer_stg = 1) 

    or (b.OverridingPayer_alfa_stg is not null))

    and ((bc_contact.updatetime_stg>(:start_dttm) 

    AND bc_contact.updatetime_stg <=(:end_dttm)) 

    OR (bc_user.updatetime_stg>(:start_dttm) 

    AND bc_user.updatetime_stg <= (:end_dttm)))



UNION



SELECT  

    pc_contact.TaxID_stg as TaxID,

    pc_contact.PublicID_stg as PublicID,

    pc_contact.AddressBookUID_stg AS LinkID,

    pc_contact.Retired_stg as Retired,

    pc_contact.CreateTime_stg as CreateTime,

    pc_contact.LicenseNumber_stg as LicenseNumber,

    pctl_contact.NAME_stg AS TL_CNT_Name,

    pc_credential.username_stg AS Partyidentificationnum,

    cast(null as varchar(255)) AS HICN_alfa,

    cast(null as varchar(255)) as AdjusterCode_alfa,

/* pc_producercode.code as agentnumber, */
    ''SRC_SYS4'' AS SYS_SRC_CD,

    cast(null as varchar(255)) as PRODUCER_CODE,

    cast(''ClaimCenter'' as varchar(255)) as SOURCE,

    case 

        when (COALESCE(pc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE)) >= COALESCE(pc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))   and

        (COALESCE(pc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(pc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE)))

        then pc_contact.updatetime_stg

        when (COALESCE(pc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(pc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )))     and 

        (COALESCE(pc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(pc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then pc_credential.updatetime_stg

        when (COALESCE(pc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(pc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))    and 

        (COALESCE(pc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(pc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then pc_user.updatetime_stg

    end as prty_idntftn_updatetime  

FROM

    DB_T_PROD_STAG.pc_contact 

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_contact    ON pctl_contact.id_stg = pc_contact.subtype_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_gendertype     ON pc_contact.gender_stg = pctl_gendertype.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_taxfilingstatustype    ON pc_contact.TaxFilingStatus_stg = pctl_taxfilingstatustype.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_taxstatus  ON pc_contact.taxstatus_stg = pctl_taxstatus.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_maritalstatus  ON pc_contact.Maritalstatus_stg = pctl_maritalstatus.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_nameprefix     ON pc_contact.prefix_stg = pctl_nameprefix.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.pctl_namesuffix     ON pc_contact.Suffix_stg = pctl_namesuffix.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.pc_user     ON pc_user.ContactID_stg = pc_contact.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.pc_credential   on pc_user.CredentialID_stg = pc_credential.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod     on pc_policyperiod.PNIContactDenorm_stg = pc_contact.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.pc_effectivedatedfields     on pc_effectivedatedfields.BranchID_stg = pc_policyperiod.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.pc_producercode     on pc_producercode.id_stg=pc_effectivedatedfields.ProducerCodeID_stg

WHERE

    pctl_contact.typecode_stg = (''UserContact'')  

    AND 

/*  below condition added to avoid duplicates*/
       pc_contact.PublicID_stg not in (''default_data:1'',

        ''systemTables:1'',''systemTables:2'') 

     AND ((pc_contact.updatetime_stg>(:start_dttm) 

    AND pc_contact.updatetime_stg <= (:end_dttm)) 

    OR (pc_user.updatetime_stg>(:start_dttm) 

    AND pc_user.updatetime_stg <= (:end_dttm)))

     

UNION



/* DB_T_PROD_STAG.CC_CONTACT */
SELECT  DISTINCT 

    cc_contact.TaxID_stg as TaxID,

    cc_contact.PublicID_stg as PublicID,

    cc_contact.AddressBookUID_stg AS LinkID,

    cc_contact.Retired_stg as Retired,

    cc_contact.CreateTime_stg as CreateTime,

    cc_contact.LicenseNumber_stg as LicenseNumber,

    cctl_contact.NAME_stg AS TL_CNT_Name,

    cc_credential.username_stg AS Partyidentificationnum,

/* cc_incident.HICN_alfa_stg AS HICN_alfa,*/
    cc_contact.HICN_alfa_stg AS HICN_alfa,

    cc_user.AdjusterCode_alfa_stg as AdjusterCode_alfa,

    ''SRC_SYS6'' AS SYS_SRC_CD,

    cast(null as varchar(255)) as PRODUCER_CODE,

    cast(''ClaimCenter'' as varchar(255)) as SOURCE,

    case 

        when (COALESCE(cc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(cc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))  and

        (COALESCE(cc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(cc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then cc_contact.updatetime_stg

        when (COALESCE(cc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(cc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )))     and 

        (COALESCE(cc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(cc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then cc_credential.updatetime_stg

        when (COALESCE(cc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(cc_contact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))    and 

        (COALESCE(cc_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(cc_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then cc_user.updatetime_stg

    end as prty_idntftn_updatetime 



FROM

    DB_T_PROD_STAG.CC_CONTACT 

    LEFT OUTER JOIN DB_T_PROD_STAG.cctl_contact    ON cctl_contact.id_stg = cc_contact.subtype_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.cctl_gendertype     ON cc_contact.gender_stg = cctl_gendertype.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.cctl_taxfilingstatustype    ON cc_contact.TaxFilingStatus_stg = cctl_taxfilingstatustype.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.cctl_taxstatus  ON cc_contact.taxstatus_stg = cctl_taxstatus.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.cctl_maritalstatus  ON cc_contact.Maritalstatus_stg = cctl_maritalstatus.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.cctl_nameprefix     ON cc_contact.prefix_stg = cctl_nameprefix.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.cctl_namesuffix     ON cc_contact.Suffix_stg = cctl_namesuffix.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.cc_user     ON cc_user.ContactID_stg = cc_contact.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.cc_credential   ON cc_user.CredentialID_stg = cc_credential.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.cc_claimcontact ON cc_contact.id_stg=cc_claimcontact.ContactID_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.cc_claimcontactrole ON cc_claimcontact.id_stg=cc_claimcontactrole.claimcontactid_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.cc_incident     ON cc_claimcontactrole.claimcontactid_stg =cc_incident.id_stg

WHERE

    (cc_contact.updatetime_stg>(:start_dttm) 

    AND cc_contact.updatetime_stg <= (:end_dttm) )  

    or 

       (cc_user.updatetime_stg>(:start_dttm) 

    AND cc_user.updatetime_stg <=(:end_dttm) )

      

UNION



SELECT  

    ab_abcontact.TaxID_stg as TaxID,

    cast(null as varchar(255)) AS PublicID,

    ab_abcontact.LinkID_stg as LinkID,  

    ab_abcontact.Retired_stg as Retired,

    ab_abcontact.CreateTime_stg as CreateTime,

    ab_abcontact.LicenseNumber_stg as LicenseNumber,

    abtl_abcontact.NAME_stg AS TL_CNT_Name,

    ab_credential.username_stg AS Partyidentificationnum,

    cast(null as varchar(255)) AS HICN_alfa,

    cast(null as varchar(255)) as AdjusterCode_alfa, 

/* abtl_occupation.typecode_stg,*/
    ''SRC_SYS7'' AS SYS_SRC_CD,

    LKP_Agent_Code.Code as PRODUCER_CODE,

    cast(''ContactManager'' as varchar(255)) as SOURCE,

    case 

        when (COALESCE(ab_abcontact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(ab_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))    and

        (COALESCE(ab_abcontact.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(ab_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then ab_abcontact.updatetime_stg

        when (COALESCE(ab_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(ab_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )))     and 

        (COALESCE(ab_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(ab_abcontact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then ab_credential.updatetime_stg

        when (COALESCE(ab_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(ab_abcontact.updatetime_stg,CAST (''1900-12-31'' AS DATE )))  and 

        (COALESCE(ab_user.updatetime_stg,CAST (''1900-12-31'' AS DATE )) >= COALESCE(ab_credential.updatetime_stg,CAST (''1900-12-31'' AS DATE )))

        then ab_user.updatetime_stg

    end as prty_idntftn_updatetime 

FROM

    DB_T_PROD_STAG.ab_abcontact

    LEFT OUTER JOIN DB_T_PROD_STAG.abtl_abcontact  ON abtl_abcontact.id_stg = ab_abcontact.subtype_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.abtl_gendertype     ON ab_abcontact.gender_stg = abtl_gendertype.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.abtl_taxfilingstatustype    ON ab_abcontact.TaxFilingStatus_stg = abtl_taxfilingstatustype.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.abtl_taxstatus  ON ab_abcontact.taxstatus_stg = abtl_taxstatus.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.abtl_maritalstatus  ON ab_abcontact.Maritalstatus_stg = abtl_maritalstatus.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.abtl_nameprefix     ON ab_abcontact.prefix_stg = abtl_nameprefix.id_stg 

    LEFT OUTER JOIN DB_T_PROD_STAG.abtl_namesuffix     ON ab_abcontact.Suffix_stg = abtl_namesuffix.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.ab_user     ON ab_user.ContactID_stg = ab_abcontact.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.ab_credential   on ab_user.CredentialID_stg = ab_credential.id_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.abtl_occupation     ON  abtl_occupation.id_stg = ab_abcontact.occupation_alfa_stg

    LEFT OUTER JOIN (   select   contact.AddressBookUID_stg AS AddressBookUID,  producer_cd.Code_stg AS Code  

    from     DB_T_PROD_STAG.pc_producercode producer_cd    

    join DB_T_PROD_STAG.pc_userproducercode user_producer      on (user_producer.ProducerCodeID_stg = producer_cd.id_stg)

    join DB_T_PROD_STAG.pc_user  user1         on(user1.id_stg= user_producer.UserID_stg)

    join DB_T_PROD_STAG.pc_contact contact         on(contact.id_stg = user1.ContactID_stg )    ) AS LKP_Agent_Code 

    on LKP_Agent_Code.AddressBookUID = ab_abcontact.LinkID_stg

    WHERE ab_abcontact.updatetime_stg>(:start_dttm) AND ab_abcontact.updatetime_stg <= (:end_dttm)

    ) main_query )

    ab_abcontact 

 WHERE

/* ab_abcontact.TL_CNT_Name IN (''Person'',''Adjudicator'',''User Contact'',''Vendor (Person)'',''Attorney'', ''Doctor'',''Policy Person'') AND*/
     ((SOURCE = ''ClaimCenter'' AND PublicID IS NOT NULL) OR (SOURCE = ''ContactManager'' AND LinkID IS NOT NULL))

AND

    ab_abcontact.AdjusterCode_alfa IS NOT NULL

    

    

    

UNION



/*  Agent Number  POL_DA_0203 */
/***** EIM-43579 Removed DB_T_PROD_STAG.pc_policyperiod, pc_job, pctl_job, DB_T_PROD_STAG.pctl_policyperiodstatus *******/

SELECT  DISTINCT

     pc_agent_x.Agent_number,

     pc_agent_x.PublicID AS PublicID,

    '''' AS LinkID,

     ''PRTY_IDNTFTN_TYPE1'' AS PRTY_IDNTFTN_TYPE_CD,

     ''ClaimCenter''  SOURCE,

    ''SRC_SYS6'' AS SYS_SRC_CD,

     createtime,

    pc_agent_x.Agent_Retired AS Retired,

    ''Person'' as TL_CNT_Name,

     updatetime 

FROM (

select  distinct code_stg as Agent_number,

pc_contact.PublicID_stg as PublicID,

case when pc_producercode.Retired_stg=0 and pc_contact.Retired_stg=0 then 0 else 1 end as Agent_Retired,

pc_producercode.createtime_stg as createtime,

pc_producercode.updatetime_stg AS updatetime 

from    DB_T_PROD_STAG.pc_producercode

left outer join DB_T_PROD_STAG.pc_userproducercode on  pc_producercode.id_stg = pc_userproducercode.producercodeid_stg

left outer join DB_T_PROD_STAG.pc_user on  pc_userproducercode.UserID_stg = pc_user.id_stg

left outer join DB_T_PROD_STAG.pc_userrole on pc_user.id_stg = pc_userrole.UserID_stg

left outer join DB_T_PROD_STAG.pc_role UserRole on pc_userrole.RoleID_stg = UserRole.ID_stg

left outer join DB_T_PROD_STAG.pc_contact on  pc_user.ContactID_stg = pc_contact.id_stg

left outer join DB_T_PROD_STAG.pctl_contact on  pc_contact.Subtype_stg = pctl_contact.ID_stg

left outer join DB_T_PROD_STAG.pc_producercoderole on  pc_producercode.id_stg = pc_producercoderole.ProducerCodeID_stg

left outer join DB_T_PROD_STAG.pc_role on  pc_producercoderole.RoleID_stg = pc_role.ID_stg

left outer join DB_T_PROD_STAG.pc_role ProducerCodeRole on  pc_producercoderole.RoleID_stg = ProducerCodeRole.ID_stg

where 

  pctl_contact.name_stg =''User Contact''

AND pc_role.name_stg =''Agent''

AND UserRole.name_stg in (''CSR'', ''Agent'')

 and (pc_producercode.UpdateTime_stg > (:start_dttm) and pc_producercode.UpdateTime_stg <= (:end_dttm))

)   pc_agent_x



) AS A
) SRC
)
);


-- Component exp_all_source, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_all_source AS
(
SELECT
CASE WHEN sq_ab_abcontact.partyidentificationnum IS NULL or ltrim ( rtrim ( sq_ab_abcontact.partyidentificationnum ) ) = '''' THEN lpad ( '' '' , 50 , '' '' ) ELSE upper ( sq_ab_abcontact.partyidentificationnum ) END as partyidentificationnum1,
sq_ab_abcontact.createtime as createtime,
LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT */ as var_prty_idntftn_type_cd,
var_prty_idntftn_type_cd as out_prty_idntftn_type_cd,
sq_ab_abcontact.createtime as prty_idntftn_strt_dttm,
LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_INTRNL_ORG_TYPE */ as INTRNL_ORG_TYPE_CD,
LKP_3.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_INTRNL_ORG_SBTYPE */ as INTRNL_ORG_SBTYPE_CD,
LKP_4.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SRC_CD */ as v_SYS_SRC_CD,
to_TIMESTAMP ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) as prty_idntftn_end_dttm,
1 as ctl_id,
:PRCS_ID   as process_id,
CURRENT_TIMESTAMP as EDW_STRT_DTTM1,
TO_TIMESTAMP ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as EDW_END_DTTM1,
sq_ab_abcontact.Retired as Retired,
CASE WHEN LKP_5.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_BUSN_CTGY_CD */ IS NULL THEN ''UNK'' ELSE LKP_6.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_BUSN_CTGY_CD */ END as var_TL_CNT_Name,
DECODE ( TRUE , sq_ab_abcontact.prty_idntftn_type_cd = ''PRTY_IDNTFTN_TYPE13'' , LKP_7.INTRNL_ORG_PRTY_ID /* replaced lookup LKP_INTRNL_ORG */ , 9999 ) as var_issug_prty_id,
var_issug_prty_id as out_issug_prty_id,
CASE
  WHEN sq_ab_abcontact.Source = ''ContactManager''
  AND sq_ab_abcontact.TL_CNT_Name IN (
    ''Person'',
    ''Adjudicator'',
    ''User Contact'',
    ''Vendor (Person)'',
    ''Attorney'',
    ''Doctor'',
    ''Policy Person'',
    ''Lodging (Person)''
  ) THEN COALESCE(LKP_9.INDIV_PRTY_ID, 9999)
  WHEN sq_ab_abcontact.Source = ''ClaimCenter''
  AND sq_ab_abcontact.TL_CNT_Name IN (
    ''Person'',
    ''Adjudicator'',
    ''User Contact'',
    ''Vendor (Person)'',
    ''Attorney'',
    ''Doctor'',
    ''Policy Person'',
    ''Lodging (Person)''
  ) THEN COALESCE(LKP_11.INDIV_PRTY_ID, 9999)
  WHEN sq_ab_abcontact.Source = ''ContactManager''
  AND NOT sq_ab_abcontact.TL_CNT_Name IN (
    ''Person'',
    ''Adjudicator'',
    ''User Contact'',
    ''Vendor (Person)'',
    ''Attorney'',
    ''Doctor'',
    ''Policy Person'',
    ''Lodging (Person)''
  ) THEN COALESCE(LKP_13.BUSN_PRTY_ID, 9999)
  WHEN sq_ab_abcontact.Source = ''ClaimCenter''
  AND NOT sq_ab_abcontact.TL_CNT_Name IN (
    ''Person'',
    ''Adjudicator'',
    ''User Contact'',
    ''Vendor (Person)'',
    ''Attorney'',
    ''Doctor'',
    ''Policy Person'',
    ''Lodging (Person)''
  ) THEN COALESCE(LKP_15.BUSN_PRTY_ID, 9999)
  ELSE 9999
END AS var_prty_id,
CASE WHEN var_prty_id IS NULL THEN 9999 ELSE var_prty_id END as out_prty_id,
CASE WHEN sq_ab_abcontact.updatetime IS NULL THEN to_date ( ''01/01/1900'' , ''mm/dd/yyyy'' ) ELSE sq_ab_abcontact.updatetime END as out_updatetime,
sq_ab_abcontact.rnk as rnk,
sq_ab_abcontact.source_record_id,
row_number() over (partition by sq_ab_abcontact.source_record_id order by sq_ab_abcontact.source_record_id) as RNK1
FROM
sq_ab_abcontact
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = sq_ab_abcontact.prty_idntftn_type_cd
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_INTRNL_ORG_TYPE LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = ''INTRNL_ORG_TYPE15''
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_INTRNL_ORG_SBTYPE LKP_3 ON LKP_3.SRC_IDNTFTN_VAL = ''root''
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_SRC_CD LKP_4 ON LKP_4.SRC_IDNTFTN_VAL = sq_ab_abcontact.SYS_SRC_CD
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_BUSN_CTGY_CD LKP_5 ON LKP_5.SRC_IDNTFTN_VAL = sq_ab_abcontact.TL_CNT_Name
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_BUSN_CTGY_CD LKP_6 ON LKP_6.SRC_IDNTFTN_VAL = sq_ab_abcontact.TL_CNT_Name
LEFT JOIN LKP_INTRNL_ORG LKP_7 ON LKP_7.INTRNL_ORG_TYPE_CD = INTRNL_ORG_TYPE_CD AND LKP_7.INTRNL_ORG_SBTYPE_CD = INTRNL_ORG_SBTYPE_CD AND LKP_7.INTRNL_ORG_NUM = ''ALFA''
LEFT JOIN LKP_INDIV_CNT_MGR LKP_8 ON LKP_8.NK_LINK_ID = sq_ab_abcontact.LinkID
LEFT JOIN LKP_INDIV_CNT_MGR LKP_9 ON LKP_9.NK_LINK_ID = sq_ab_abcontact.LinkID
LEFT JOIN LKP_INDIV_CLM_CTR LKP_10 ON LKP_10.NK_PUBLC_ID = sq_ab_abcontact.PublicID
LEFT JOIN LKP_INDIV_CLM_CTR LKP_11 ON LKP_11.NK_PUBLC_ID = sq_ab_abcontact.PublicID
LEFT JOIN LKP_BUSN LKP_12 ON LKP_12.BUSN_CTGY_CD = var_TL_CNT_Name AND LKP_12.NK_BUSN_CD = sq_ab_abcontact.LinkID
LEFT JOIN LKP_BUSN LKP_13 ON LKP_13.BUSN_CTGY_CD = var_TL_CNT_Name AND LKP_13.NK_BUSN_CD = sq_ab_abcontact.LinkID
LEFT JOIN LKP_BUSN LKP_14 ON LKP_14.BUSN_CTGY_CD = var_TL_CNT_Name AND LKP_14.NK_BUSN_CD = sq_ab_abcontact.PublicID
LEFT JOIN LKP_BUSN LKP_15 ON LKP_15.BUSN_CTGY_CD = var_TL_CNT_Name AND LKP_15.NK_BUSN_CD = sq_ab_abcontact.PublicID
QUALIFY RNK1 = 1
);


-- Component LKP_PRTY_IDNTFN_CDC, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_PRTY_IDNTFN_CDC AS
(
SELECT
LKP.ISSUG_PRTY_ID,
LKP.PRTY_IDNTFTN_TYPE_CD,
LKP.PRTY_IDNTFTN_STRT_DTTM,
LKP.PRTY_ID,
LKP.PRTY_IDNTFTN_END_DTTM,
LKP.PRTY_IDNTFTN_NUM,
LKP.EDW_STRT_DTTM,
LKP.EDW_END_DTTM,
exp_all_source.out_prty_idntftn_type_cd as out_prty_idntftn_type_cd,
exp_all_source.out_issug_prty_id as out_issug_prty_id,
exp_all_source.out_prty_id as out_prty_id,
exp_all_source.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_all_source.source_record_id ORDER BY LKP.ISSUG_PRTY_ID desc,LKP.PRTY_IDNTFTN_TYPE_CD desc,LKP.PRTY_IDNTFTN_STRT_DTTM desc,LKP.PRTY_ID desc,LKP.PRTY_IDNTFTN_END_DTTM desc,LKP.PRTY_IDNTFTN_NUM desc,LKP.EDW_STRT_DTTM desc,LKP.EDW_END_DTTM desc) RNK
FROM
exp_all_source
LEFT JOIN (
SELECT PRTY_IDNTFTN.PRTY_IDNTFTN_STRT_DTTM as PRTY_IDNTFTN_STRT_DTTM, PRTY_IDNTFTN.PRTY_IDNTFTN_END_DTTM as PRTY_IDNTFTN_END_DTTM, PRTY_IDNTFTN.PRTY_IDNTFTN_NUM as PRTY_IDNTFTN_NUM, PRTY_IDNTFTN.EDW_STRT_DTTM as EDW_STRT_DTTM, PRTY_IDNTFTN.EDW_END_DTTM as EDW_END_DTTM, PRTY_IDNTFTN.ISSUG_PRTY_ID as ISSUG_PRTY_ID, PRTY_IDNTFTN.PRTY_IDNTFTN_TYPE_CD as PRTY_IDNTFTN_TYPE_CD, PRTY_IDNTFTN.PRTY_ID as PRTY_ID FROM DB_T_PROD_CORE.PRTY_IDNTFTN QUALIFY ROW_NUMBER() OVER(PARTITION BY  PRTY_IDNTFTN.ISSUG_PRTY_ID,PRTY_IDNTFTN.PRTY_IDNTFTN_TYPE_CD,PRTY_IDNTFTN.PRTY_ID ORDER BY PRTY_IDNTFTN.EDW_END_DTTM DESC) = 1
) LKP ON LKP.ISSUG_PRTY_ID = exp_all_source.out_issug_prty_id AND LKP.PRTY_IDNTFTN_TYPE_CD = exp_all_source.out_prty_idntftn_type_cd AND LKP.PRTY_ID = exp_all_source.out_prty_id
QUALIFY RNK = 1
);


-- Component exp_set_flag, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_set_flag AS
(
SELECT
LKP_PRTY_IDNTFN_CDC.out_prty_idntftn_type_cd as prty_idntftn_type_cd,
exp_all_source.prty_idntftn_strt_dttm as prty_idntftn_strt_dttm,
exp_all_source.prty_idntftn_end_dttm as prty_idntftn_end_dttm,
exp_all_source.partyidentificationnum1 as partyidentificationnum,
upper ( exp_all_source.partyidentificationnum1 ) as partyidentificationnum_var,
LKP_PRTY_IDNTFN_CDC.out_issug_prty_id as out_issug_prty_id,
LKP_PRTY_IDNTFN_CDC.out_prty_id as out_prty_id,
exp_all_source.ctl_id as ctl_id,
exp_all_source.process_id as process_id,
LKP_PRTY_IDNTFN_CDC.PRTY_IDNTFTN_NUM as lkp_PRTY_IDNTFTN_NUM,
upper ( LKP_PRTY_IDNTFN_CDC.PRTY_IDNTFTN_NUM ) as lkp_PRTY_IDNTFTN_NUM_var,
LKP_PRTY_IDNTFN_CDC.PRTY_IDNTFTN_STRT_DTTM as lkp_PRTY_IDNTFTN_STRT_DTTM,
LKP_PRTY_IDNTFN_CDC.PRTY_IDNTFTN_END_DTTM as lkp_PRTY_IDNTFTN_END_DTTM,
md5 ( ltrim ( rtrim ( lkp_PRTY_IDNTFTN_NUM_var ) ) || ltrim ( rtrim ( LKP_PRTY_IDNTFN_CDC.PRTY_IDNTFTN_STRT_DTTM ) ) || ltrim ( rtrim ( LKP_PRTY_IDNTFN_CDC.PRTY_IDNTFTN_END_DTTM ) ) ) as chksum_lkp,
md5 ( ltrim ( rtrim ( partyidentificationnum_var ) ) || ltrim ( rtrim ( exp_all_source.prty_idntftn_strt_dttm ) ) || ltrim ( rtrim ( exp_all_source.prty_idntftn_end_dttm ) ) ) as chksum_inp,
CASE WHEN chksum_lkp IS NULL THEN ''I'' ELSE CASE WHEN chksum_lkp != chksum_inp THEN ''U'' ELSE ''R'' END END as out_flag,
LKP_PRTY_IDNTFN_CDC.EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
LKP_PRTY_IDNTFN_CDC.ISSUG_PRTY_ID as lkp_ISSUG_PRTY_ID,
exp_all_source.EDW_STRT_DTTM1 as EDW_STRT_DTTM,
exp_all_source.EDW_END_DTTM1 as EDW_END_DTTM,
LKP_PRTY_IDNTFN_CDC.PRTY_IDNTFTN_TYPE_CD as lkpPRTY_IDNTFTN_TYPE_CD1,
LKP_PRTY_IDNTFN_CDC.PRTY_ID as lkpPRTY_ID,
exp_all_source.Retired as Retired,
LKP_PRTY_IDNTFN_CDC.EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_all_source.out_updatetime as updatetime,
exp_all_source.rnk as rnk,
exp_all_source.source_record_id
FROM
exp_all_source
INNER JOIN LKP_PRTY_IDNTFN_CDC ON exp_all_source.source_record_id = LKP_PRTY_IDNTFN_CDC.source_record_id
);


-- Component rtr_insert_update_flag_Retire, Type ROUTER Output Group Retire
CREATE OR REPLACE TEMPORARY TABLE rtr_insert_update_flag_Retire AS
(SELECT
exp_set_flag.prty_idntftn_type_cd as prty_idntftn_type_cd,
exp_set_flag.prty_idntftn_strt_dttm as prty_idntftn_strt_dttm,
exp_set_flag.prty_idntftn_end_dttm as prty_idntftn_end_dttm,
exp_set_flag.partyidentificationnum as partyidentificationnum,
exp_set_flag.out_issug_prty_id as out_issug_prty_id,
exp_set_flag.out_prty_id as out_prty_id,
exp_set_flag.ctl_id as ctl_id,
exp_set_flag.process_id as process_id,
exp_set_flag.out_flag as out_flag,
exp_set_flag.lkp_PRTY_IDNTFTN_STRT_DTTM as lkp_PRTY_IDNTFTN_STRT_DTTM,
exp_set_flag.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_set_flag.EDW_STRT_DTTM as EDW_STRT_DTTM1,
exp_set_flag.EDW_END_DTTM as EDW_END_DTTM,
exp_set_flag.lkp_PRTY_IDNTFTN_NUM as lkp_PRTY_IDNTFTN_NUM,
exp_set_flag.lkp_PRTY_IDNTFTN_END_DTTM as lkp_PRTY_IDNTFTN_END_DTTM,
exp_set_flag.lkp_ISSUG_PRTY_ID as lkp_ISSUG_PRTY_ID,
exp_set_flag.lkpPRTY_IDNTFTN_TYPE_CD1 as lkpPRTY_IDNTFTN_TYPE_CD1,
exp_set_flag.lkpPRTY_ID as lkpPRTY_ID,
exp_set_flag.Retired as Retired,
exp_set_flag.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_set_flag.updatetime as updatetime,
exp_set_flag.rnk as rnk,
exp_set_flag.source_record_id
FROM
exp_set_flag
WHERE exp_set_flag.out_flag = ''R'' and exp_set_flag.Retired != 0 and exp_set_flag.lkp_EDW_END_DTTM = TO_timestamp ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ));


-- Component rtr_insert_update_flag_insert, Type ROUTER Output Group insert
CREATE OR REPLACE TEMPORARY TABLE rtr_insert_update_flag_insert AS
(SELECT
exp_set_flag.prty_idntftn_type_cd as prty_idntftn_type_cd,
exp_set_flag.prty_idntftn_strt_dttm as prty_idntftn_strt_dttm,
exp_set_flag.prty_idntftn_end_dttm as prty_idntftn_end_dttm,
exp_set_flag.partyidentificationnum as partyidentificationnum,
exp_set_flag.out_issug_prty_id as out_issug_prty_id,
exp_set_flag.out_prty_id as out_prty_id,
exp_set_flag.ctl_id as ctl_id,
exp_set_flag.process_id as process_id,
exp_set_flag.out_flag as out_flag,
exp_set_flag.lkp_PRTY_IDNTFTN_STRT_DTTM as lkp_PRTY_IDNTFTN_STRT_DTTM,
exp_set_flag.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_set_flag.EDW_STRT_DTTM as EDW_STRT_DTTM1,
exp_set_flag.EDW_END_DTTM as EDW_END_DTTM,
exp_set_flag.lkp_PRTY_IDNTFTN_NUM as lkp_PRTY_IDNTFTN_NUM,
exp_set_flag.lkp_PRTY_IDNTFTN_END_DTTM as lkp_PRTY_IDNTFTN_END_DTTM,
exp_set_flag.lkp_ISSUG_PRTY_ID as lkp_ISSUG_PRTY_ID,
exp_set_flag.lkpPRTY_IDNTFTN_TYPE_CD1 as lkpPRTY_IDNTFTN_TYPE_CD1,
exp_set_flag.lkpPRTY_ID as lkpPRTY_ID,
exp_set_flag.Retired as Retired,
exp_set_flag.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_set_flag.updatetime as updatetime,
exp_set_flag.rnk as rnk,
exp_set_flag.source_record_id
FROM
exp_set_flag
WHERE ( ( exp_set_flag.out_flag = ''I'' OR exp_set_flag.out_flag = ''U'' ) AND exp_set_flag.out_prty_id <> 9999 AND exp_set_flag.out_prty_id <> 9999 AND exp_set_flag.out_issug_prty_id IS NOT NULL ) OR ( exp_set_flag.Retired = 0 AND exp_set_flag.lkp_EDW_END_DTTM != TO_timestamp ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ));


-- Component upd_prty_idfn_insert, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_prty_idfn_insert AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_insert_update_flag_insert.prty_idntftn_type_cd as prty_idntftn_type_cd1,
rtr_insert_update_flag_insert.prty_idntftn_strt_dttm as prty_idntftn_strt_dttm1,
rtr_insert_update_flag_insert.prty_idntftn_end_dttm as prty_idntftn_end_dttm1,
rtr_insert_update_flag_insert.partyidentificationnum as partyidentificationnum1,
rtr_insert_update_flag_insert.out_issug_prty_id as out_issug_prty_id1,
rtr_insert_update_flag_insert.out_prty_id as out_prty_id1,
rtr_insert_update_flag_insert.ctl_id as ctl_id1,
rtr_insert_update_flag_insert.process_id as process_id1,
rtr_insert_update_flag_insert.EDW_STRT_DTTM1 as EDW_STRT_DTTM11,
rtr_insert_update_flag_insert.EDW_END_DTTM as EDW_END_DTTM1,
rtr_insert_update_flag_insert.Retired as Retired1,
rtr_insert_update_flag_insert.updatetime as updatetime1,
rtr_insert_update_flag_insert.rnk as rnk1,
0 as UPDATE_STRATEGY_ACTION,
rtr_insert_update_flag_insert.source_record_id
FROM
rtr_insert_update_flag_insert
);


-- Component exp_pass_to_target_insert, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_target_insert AS
(
SELECT
upd_prty_idfn_insert.prty_idntftn_type_cd1 as prty_idntftn_type_cd1,
upd_prty_idfn_insert.prty_idntftn_strt_dttm1 as prty_idntftn_strt_dttm1,
upd_prty_idfn_insert.prty_idntftn_end_dttm1 as prty_idntftn_end_dttm1,
upd_prty_idfn_insert.partyidentificationnum1 as partyidentificationnum1,
upd_prty_idfn_insert.out_issug_prty_id1 as out_issug_prty_id1,
upd_prty_idfn_insert.out_prty_id1 as out_prty_id1,
upd_prty_idfn_insert.process_id1 as process_id1,
upd_prty_idfn_insert.EDW_END_DTTM1 as EDW_END_DTTM1,
upd_prty_idfn_insert.updatetime1 as updatetime1,
CASE WHEN upd_prty_idfn_insert.Retired1 = 0 THEN DATEADD (
  SECOND,
  2 * (upd_prty_idfn_insert.rnk1 - 1),
  CURRENT_TIMESTAMP()
) ELSE CURRENT_TIMESTAMP END as o_EDW_STRT_DTTM,
upd_prty_idfn_insert.source_record_id
FROM
upd_prty_idfn_insert
);


-- Component upd_tgt_prty_idntftn_Retire_Reject, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_tgt_prty_idntftn_Retire_Reject AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_insert_update_flag_Retire.lkp_ISSUG_PRTY_ID as lkp_ISSUG_PRTY_ID3,
rtr_insert_update_flag_Retire.lkpPRTY_IDNTFTN_TYPE_CD1 as lkpPRTY_IDNTFTN_TYPE_CD13,
rtr_insert_update_flag_Retire.lkp_PRTY_IDNTFTN_STRT_DTTM as lkp_PRTY_IDNTFTN_STRT_DTTM3,
rtr_insert_update_flag_Retire.lkpPRTY_ID as lkpPRTY_ID3,
rtr_insert_update_flag_Retire.lkp_PRTY_IDNTFTN_END_DTTM as lkp_PRTY_IDNTFTN_END_DTTM3,
rtr_insert_update_flag_Retire.lkp_PRTY_IDNTFTN_NUM as lkp_PRTY_IDNTFTN_NUM3,
rtr_insert_update_flag_Retire.process_id as process_id3,
rtr_insert_update_flag_Retire.lkp_EDW_STRT_DTTM as EDW_STRT_DTTM13,
1 as UPDATE_STRATEGY_ACTION,
rtr_insert_update_flag_Retire.source_record_id
FROM
rtr_insert_update_flag_Retire
);


-- Component tgt_prty_idntftn_insert, Type TARGET 
INSERT INTO DB_T_PROD_CORE.PRTY_IDNTFTN
(
ISSUG_PRTY_ID,
PRTY_IDNTFTN_TYPE_CD,
PRTY_IDNTFTN_STRT_DTTM,
PRTY_ID,
PRTY_IDNTFTN_END_DTTM,
PRTY_IDNTFTN_NUM,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM
)
SELECT
exp_pass_to_target_insert.out_issug_prty_id1 as ISSUG_PRTY_ID,
exp_pass_to_target_insert.prty_idntftn_type_cd1 as PRTY_IDNTFTN_TYPE_CD,
exp_pass_to_target_insert.prty_idntftn_strt_dttm1 as PRTY_IDNTFTN_STRT_DTTM,
exp_pass_to_target_insert.out_prty_id1 as PRTY_ID,
exp_pass_to_target_insert.prty_idntftn_end_dttm1 as PRTY_IDNTFTN_END_DTTM,
exp_pass_to_target_insert.partyidentificationnum1 as PRTY_IDNTFTN_NUM,
exp_pass_to_target_insert.process_id1 as PRCS_ID,
exp_pass_to_target_insert.o_EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_pass_to_target_insert.EDW_END_DTTM1 as EDW_END_DTTM,
exp_pass_to_target_insert.updatetime1 as TRANS_STRT_DTTM
FROM
exp_pass_to_target_insert;


-- Component tgt_prty_idntftn_insert, Type Post SQL 
UPDATE  DB_T_PROD_CORE.PRTY_IDNTFTN FROM

(

SELECT  DISTINCT ISSUG_PRTY_ID, PRTY_IDNTFTN_TYPE_CD, PRTY_ID,TRANS_STRT_DTTM,EDW_STRT_DTTM,EDW_END_DTTM,

MAX(EDW_STRT_DTTM) OVER (PARTITION BY ISSUG_PRTY_ID, PRTY_IDNTFTN_TYPE_CD, PRTY_ID ORDER BY trans_strt_dttm,edw_strt_dttm ASC ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) - INTERVAL ''1 SECOND'' 

 AS LEAD1, 

MAX(TRANS_STRT_DTTM) OVER (PARTITION BY ISSUG_PRTY_ID, PRTY_IDNTFTN_TYPE_CD, PRTY_ID ORDER BY trans_strt_dttm,edw_strt_dttm ASC ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) - INTERVAL ''1 SECOND'' 

 AS LEAD

FROM DB_T_PROD_CORE.PRTY_IDNTFTN WHERE PRTY_IDNTFTN_TYPE_CD<>''AGT''  

 ) A

SET TRANS_END_DTTM=  A.LEAD,

EDW_END_DTTM=A.lead1

WHERE  PRTY_IDNTFTN.EDW_STRT_DTTM = A.EDW_STRT_DTTM

AND PRTY_IDNTFTN.TRANS_STRT_DTTM = A.TRANS_STRT_DTTM

AND A.LEAD  IS NOT NULL 

AND  PRTY_IDNTFTN.ISSUG_PRTY_ID=A.ISSUG_PRTY_ID

AND  PRTY_IDNTFTN.PRTY_IDNTFTN_TYPE_CD=A.PRTY_IDNTFTN_TYPE_CD

AND  PRTY_IDNTFTN.PRTY_ID=A.PRTY_ID

AND PRTY_IDNTFTN.TRANS_STRT_DTTM<>PRTY_IDNTFTN.TRANS_END_DTTM;



UPDATE  DB_T_PROD_CORE.PRTY_IDNTFTN FROM

(

SELECT  DISTINCT ISSUG_PRTY_ID, PRTY_IDNTFTN_TYPE_CD, PRTY_ID,TRANS_STRT_DTTM,EDW_STRT_DTTM,EDW_END_DTTM,

MAX(EDW_STRT_DTTM) OVER (PARTITION BY ISSUG_PRTY_ID, PRTY_IDNTFTN_TYPE_CD, PRTY_ID ORDER BY trans_strt_dttm,edw_strt_dttm ASC ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) - INTERVAL ''0.001 SECOND''

 AS LEAD1, 

MAX(TRANS_STRT_DTTM) OVER (PARTITION BY ISSUG_PRTY_ID, PRTY_IDNTFTN_TYPE_CD, PRTY_ID ORDER BY trans_strt_dttm,edw_strt_dttm ASC ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) - INTERVAL ''0.001 SECOND''

 AS LEAD

FROM DB_T_PROD_CORE.PRTY_IDNTFTN WHERE PRTY_IDNTFTN_TYPE_CD=''AGT''

 ) A

SET TRANS_END_DTTM=  A.LEAD,

EDW_END_DTTM=A.lead1

WHERE  PRTY_IDNTFTN.EDW_STRT_DTTM = A.EDW_STRT_DTTM

AND PRTY_IDNTFTN.TRANS_STRT_DTTM = A.TRANS_STRT_DTTM

AND A.LEAD  IS NOT NULL 

AND  PRTY_IDNTFTN.ISSUG_PRTY_ID=A.ISSUG_PRTY_ID

AND  PRTY_IDNTFTN.PRTY_IDNTFTN_TYPE_CD=A.PRTY_IDNTFTN_TYPE_CD

AND  PRTY_IDNTFTN.PRTY_ID=A.PRTY_ID

AND PRTY_IDNTFTN.TRANS_STRT_DTTM<>PRTY_IDNTFTN.TRANS_END_DTTM;



/*    update EDW_END_DTTM  with 1 sec less for  duplicate records loaded with diff in start dt */

/*

UPDATE  DB_T_PROD_CORE.PRTY_IDNTFTN  FROM  

(

SELECT	DISTINCT 	ISSUG_PRTY_ID, PRTY_IDNTFTN_TYPE_CD, PRTY_ID,EDW_STRT_DTTM, TRANS_STRT_DTTM

FROM	DB_T_PROD_CORE.PRTY_IDNTFTN

WHERE EDW_END_DTTM=TO_DATE(''9999/31/12'',''YYYY/DD/MM'') 

QUALIFY ROW_NUMBER() OVER(PARTITION BY ISSUG_PRTY_ID, PRTY_IDNTFTN_TYPE_CD, PRTY_ID ORDER BY TRANS_STRT_DTTM DESC ) >1

)  A

SET EDW_END_DTTM= A.EDW_STRT_DTTM + INTERVAL ''1 SECOND'' 

WHERE  PRTY_IDNTFTN.ISSUG_PRTY_ID=A.ISSUG_PRTY_ID

AND  PRTY_IDNTFTN.PRTY_IDNTFTN_TYPE_CD=A.PRTY_IDNTFTN_TYPE_CD

AND  PRTY_IDNTFTN.PRTY_ID=A.PRTY_ID

AND  PRTY_IDNTFTN.TRANS_STRT_DTTM=A.TRANS_STRT_DTTM

AND  PRTY_IDNTFTN.EDW_END_DTTM=TO_DATE(''9999/31/12'',''YYYY/DD/MM'');

*/



/*   Transaction Dat end Dateing */

/*

UPDATE  DB_T_PROD_CORE.PRTY_IDNTFTN FROM

(

SELECT	DISTINCT ISSUG_PRTY_ID, PRTY_IDNTFTN_TYPE_CD, PRTY_ID,TRANS_STRT_DTTM,EDW_STRT_DTTM,EDW_END_DTTM,

MAX(TRANS_STRT_DTTM) OVER (PARTITION BY ISSUG_PRTY_ID, PRTY_IDNTFTN_TYPE_CD, PRTY_ID ORDER BY TRANS_STRT_DTTM ASC ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) - INTERVAL ''1 SECOND'' 

 AS LEAD

FROM	DB_T_PROD_CORE.PRTY_IDNTFTN  

WHERE  TRANS_END_DTTM=TO_DATE(''9999/31/12'',''YYYY/DD/MM'') 

 ) A

SET TRANS_END_DTTM=  A.LEAD

WHERE  PRTY_IDNTFTN.EDW_STRT_DTTM = A.EDW_STRT_DTTM

AND A.LEAD  IS NOT NULL 

AND  PRTY_IDNTFTN.ISSUG_PRTY_ID=A.ISSUG_PRTY_ID

AND  PRTY_IDNTFTN.PRTY_IDNTFTN_TYPE_CD=A.PRTY_IDNTFTN_TYPE_CD

AND  PRTY_IDNTFTN.PRTY_ID=A.PRTY_ID

AND PRTY_IDNTFTN.EDW_END_DTTM<>TO_DATE(''9999/31/12'',''YYYY/DD/MM'');

*/;


-- Component exp_prty_idfn_update_Retire_Reject, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_prty_idfn_update_Retire_Reject AS
(
SELECT
upd_tgt_prty_idntftn_Retire_Reject.lkp_ISSUG_PRTY_ID3 as lkp_ISSUG_PRTY_ID3,
upd_tgt_prty_idntftn_Retire_Reject.lkpPRTY_IDNTFTN_TYPE_CD13 as lkpPRTY_IDNTFTN_TYPE_CD13,
upd_tgt_prty_idntftn_Retire_Reject.lkp_PRTY_IDNTFTN_STRT_DTTM3 as lkp_PRTY_IDNTFTN_STRT_DTTM3,
upd_tgt_prty_idntftn_Retire_Reject.lkpPRTY_ID3 as lkpPRTY_ID3,
upd_tgt_prty_idntftn_Retire_Reject.lkp_PRTY_IDNTFTN_END_DTTM3 as lkp_PRTY_IDNTFTN_END_DTTM3,
upd_tgt_prty_idntftn_Retire_Reject.lkp_PRTY_IDNTFTN_NUM3 as lkp_PRTY_IDNTFTN_NUM3,
upd_tgt_prty_idntftn_Retire_Reject.EDW_STRT_DTTM13 as EDW_STRT_DTTM13,
CURRENT_TIMESTAMP as o_EDW_END_DTTM,
upd_tgt_prty_idntftn_Retire_Reject.source_record_id
FROM
upd_tgt_prty_idntftn_Retire_Reject
);


-- Component tgt_prty_idntftn_upd_Update_Retire_Reject, Type TARGET 
MERGE INTO DB_T_PROD_CORE.PRTY_IDNTFTN
USING exp_prty_idfn_update_Retire_Reject ON (PRTY_IDNTFTN.ISSUG_PRTY_ID = exp_prty_idfn_update_Retire_Reject.lkp_ISSUG_PRTY_ID3 AND PRTY_IDNTFTN.PRTY_IDNTFTN_TYPE_CD = exp_prty_idfn_update_Retire_Reject.lkpPRTY_IDNTFTN_TYPE_CD13 AND PRTY_IDNTFTN.PRTY_IDNTFTN_STRT_DTTM = exp_prty_idfn_update_Retire_Reject.lkp_PRTY_IDNTFTN_STRT_DTTM3 AND PRTY_IDNTFTN.PRTY_ID = exp_prty_idfn_update_Retire_Reject.lkpPRTY_ID3 AND PRTY_IDNTFTN.PRTY_IDNTFTN_END_DTTM = exp_prty_idfn_update_Retire_Reject.lkp_PRTY_IDNTFTN_END_DTTM3 AND PRTY_IDNTFTN.PRTY_IDNTFTN_NUM = exp_prty_idfn_update_Retire_Reject.lkp_PRTY_IDNTFTN_NUM3 AND PRTY_IDNTFTN.EDW_STRT_DTTM = exp_prty_idfn_update_Retire_Reject.EDW_STRT_DTTM13)
WHEN MATCHED THEN UPDATE
SET
ISSUG_PRTY_ID = exp_prty_idfn_update_Retire_Reject.lkp_ISSUG_PRTY_ID3,
PRTY_IDNTFTN_TYPE_CD = exp_prty_idfn_update_Retire_Reject.lkpPRTY_IDNTFTN_TYPE_CD13,
PRTY_IDNTFTN_STRT_DTTM = exp_prty_idfn_update_Retire_Reject.lkp_PRTY_IDNTFTN_STRT_DTTM3,
PRTY_ID = exp_prty_idfn_update_Retire_Reject.lkpPRTY_ID3,
PRTY_IDNTFTN_END_DTTM = exp_prty_idfn_update_Retire_Reject.lkp_PRTY_IDNTFTN_END_DTTM3,
PRTY_IDNTFTN_NUM = exp_prty_idfn_update_Retire_Reject.lkp_PRTY_IDNTFTN_NUM3,
EDW_STRT_DTTM = exp_prty_idfn_update_Retire_Reject.EDW_STRT_DTTM13,
EDW_END_DTTM = exp_prty_idfn_update_Retire_Reject.o_EDW_END_DTTM,
TRANS_END_DTTM = exp_prty_idfn_update_Retire_Reject.o_EDW_END_DTTM;


INSERT INTO control_status (run_id, worklet_name, task_name, task_status, task_start_dttm, task_end_dttm, var_json)
SELECT :run_id, :worklet_name, ''m_base_prty_idntftn_insupd'', ''SUCCEEDED'', :v_start_time, CURRENT_TIMESTAMP(), OBJECT_CONSTRUCT(
  ''start_dttm'', :start_dttm,
  ''end_dttm'', :end_dttm,
  ''StartTime'', :v_start_time
);

EXCEPTION WHEN OTHER THEN
    INSERT INTO control_status (run_id, worklet_name, task_name, task_status, task_start_dttm, task_end_dttm, var_json)
    SELECT :run_id, :worklet_name, ''m_base_prty_idntftn_insupd'', ''FAILED'', :v_start_time, CURRENT_TIMESTAMP(), OBJECT_CONSTRUCT(
        ''SQLERRM'', :sqlerrm
    );


END; ';