-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_XREF_PRTY("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
DECLARE
  END_DTTM STRING;
  START_DTTM STRING;
  run_id STRING;
  workflow_name STRING;
  session_name STRING;
BEGIN
 run_id :=   (SELECT run_id   FROM control_run_id where worklet_name= :worklet_name order by insert_ts desc limit 1);   
 END_DTTM:=   (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''END_DTTM'');
 START_DTTM:=     (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''START_DTTM'');

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


-- Component LKP_XREF_PRTY, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_XREF_PRTY AS
(
SELECT
PRTY_ID,
DIR_PRTY_VAL,
INDIV_SRC_VAL,
NK_LNK_ID,
NK_PUBLC_ID,
BUSN_CTGY_CD,
NK_BUSN_VAL,
INTRNL_ORG_TYPE_CD,
INTRNL_ORG_SBTYPE_CD,
INTRNL_ORG_NUM,
SRC_SYS_CD
FROM DB_T_PROD_CORE.DIR_PRTY
);


-- Component sq_xref_prty, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_xref_prty AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as PRTY_TYPE,
$2 as INDIV_SRC_SYS,
$3 as NK_LINK_ID,
$4 as NK_PUBLC_ID,
$5 as BUSN_CTGY_CD,
$6 as NK_BUSN_CD,
$7 as INTRNL_ORG_TYPE_CD,
$8 as INTRNL_ORG_SBTYPE_CD,
$9 as INTRNL_ORG_NUM,
$10 as SRC_SYS_CD,
$11 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT DISTINCT													

      CAST(''INDIV'' AS VARCHAR(50)) AS PRTY_TYPE,													

      SOURCE,													

      cast(CASE WHEN ab_abcontact.Source = ''ContactManager'' THEN UPPER(ab_abcontact.LinkID) 													

      ELSE NULL END  as varchar(64))  AS LinkID,													

      cast(CASE WHEN ab_abcontact.Source = ''ClaimCenter'' THEN ab_abcontact.PublicID													

      ELSE NULL END as varchar(64))  AS PublicID,													

      CAST(NULL AS VARCHAR(50)) AS BUSN_CTGY_CD,													

      CAST(NULL AS VARCHAR(50)) AS NK_BUSN_CD,													

      CAST(NULL AS VARCHAR(50)) AS INTRNL_ORG_TYPE_CD,													

     CAST(NULL AS VARCHAR(50)) AS INTRNL_ORG_SBTYPE_CD,													

      CAST(NULL AS VARCHAR(50)) AS INTRNL_ORG_NUM,													

      SYS_SRC_CD,													

      CAST(1 AS INTEGER) AS sort													

FROM													

      (SELECT 													

      cast(''ClaimCenter'' as varchar(50))AS Source,													

      cast(bc_contact.PublicID_stg as varchar(64)) AS PublicID, 													

      cast(bc_contact.AddressBookUID_stg as varchar(64)) AS LinkID, 													

      ''SRC_SYS5'' AS SYS_SRC_CD,													

      bctl_contact.NAME_stg AS TL_CNT_Name,													

      (:start_dttm) as start_dttm,													

      (:end_dttm) as end_dttm													

FROM													

      DB_T_PROD_STAG.bc_contact 													

      LEFT OUTER JOIN DB_T_PROD_STAG.bc_user ON bc_user.ContactID_stg = bc_contact.id_stg													

      LEFT OUTER JOIN DB_T_PROD_STAG.bctl_contact ON bctl_contact.id_stg = bc_contact.subtype_stg													

      WHERE													

      bctl_contact.typecode_stg = (''UserContact'')  AND 													

    bc_contact.publicid_stg not in (''default_data:1'', ''systemTables:1'',''systemTables:2'') 													

      and ((bc_contact.UpdateTime_stg>(:start_dttm) AND bc_contact.UpdateTime_stg <=(:end_dttm)) OR (bc_user.UpdateTime_stg>(:start_dttm) AND bc_user.UpdateTime_stg <= (:end_dttm)))													

UNION													

													

SELECT     													

      ''ClaimCenter'' AS Source,													

      cast(case 													

             when(bc_contact.ExternalID_stg is not null) then bc_contact.ExternalID_stg 													

             else bc_contact.PublicID_stg 													

             end as varchar(64)) AS PublicID, 													

      cast(bc_contact.AddressBookUID_stg as varchar(64)) AS LinkID, 													

      ''SRC_SYS5'' AS SYS_SRC_CD,													

      bctl_contact.NAME_stg AS TL_CNT_Name,													

      (:start_dttm) as start_dttm,													

      (:end_dttm) as end_dttm													

from     DB_T_PROD_STAG.bc_account a													

inner join DB_T_PROD_STAG.bc_accountcontact h on h.AccountID_stg = a.id_stg													

inner join DB_T_PROD_STAG.bc_contact on bc_contact.id_stg = h.ContactID_stg													

join DB_T_PROD_STAG.bctl_contact on bctl_contact.ID_stg=bc_contact.Subtype_stg													

left join DB_T_PROD_STAG.bc_accountcontactrole i on i.AccountContactID_stg = h.id_stg													

left join DB_T_PROD_STAG.bctl_accountrole j on j.id_stg = i.Role_stg													

where   ((h.PrimaryPayer_stg = 1) 													

      or (j.name_stg = ''Payer''))													

      and ((bc_contact.UpdateTime_stg>(:start_dttm) 

      AND bc_contact.UpdateTime_stg <=(:end_dttm)) 

      /*OR (bc_user.UpdateTime_stg>(:start_dttm) 

      AND bc_user.UpdateTime_stg <= (:end_dttm))*/

      OR (a.UpdateTime_stg>(:start_dttm) 

      AND a.UpdateTime_stg <= (:end_dttm)))

UNION													

													

SELECT 													

      ''ClaimCenter'' AS Source,													

      cast(case when (bc_contact.ExternalID_stg is null) then bc_contact.PublicID_stg else bc_contact.ExternalID_stg end  as varchar(64))AS PublicID, 													

      cast(bc_contact.AddressBookUID_stg as varchar(64)) AS LinkID, 													

      ''SRC_SYS5'' AS SYS_SRC_CD,													

      bctl_contact.NAME_stg AS TL_CNT_Name,													

      (:start_dttm) as start_dttm,													

      (:end_dttm) as end_dttm													

from DB_T_PROD_STAG.bc_account a													

inner join DB_T_PROD_STAG.bc_invoicestream b on a.id_stg = b.AccountID_stg													

inner join DB_T_PROD_STAG.bc_accountcontact c on c.accountid_stg=a.id_stg													

inner join DB_T_PROD_STAG.bc_contact on bc_contact.id_stg = c.ContactID_stg													

join DB_T_PROD_STAG.bctl_contact on bctl_contact.ID_stg=bc_contact.Subtype_stg													

left join DB_T_PROD_STAG.bc_accountcontactrole f on f.AccountContactID_stg = c.id_stg													

left join DB_T_PROD_STAG.bctl_accountrole g on g.id_stg = f.Role_stg													

where ((b.OverridingPayer_alfa_stg is null and c.PrimaryPayer_stg = 1) or (b.OverridingPayer_alfa_stg is not null))													

and ((bc_contact.UpdateTime_stg>(:start_dttm) AND bc_contact.UpdateTime_stg <=(:end_dttm))

/*OR (bc_user.UpdateTime_stg>(:start_dttm) AND bc_user.UpdateTime_stg <= (:end_dttm))*/

OR (a.UpdateTime_stg>(:start_dttm) AND a.UpdateTime_stg <= (:end_dttm)))

UNION													

													

SELECT     													

''ClaimCenter'' AS Source,													

pc_contact.PublicID_stg As publicid,													

cast(pc_contact.AddressBookUID_stg as varchar(64)) AS LinkID, 													

''SRC_SYS4'' AS SYS_SRC_CD,													

pctl_contact.NAME_stg AS TL_CNT_Name,													

(:start_dttm) as start_dttm,													

(:end_dttm) as end_dttm													

FROM													

      DB_T_PROD_STAG.pc_contact 													

      LEFT OUTER JOIN DB_T_PROD_STAG.pc_user ON pc_user.ContactID_stg = pc_contact.id_stg													

      LEFT OUTER JOIN DB_T_PROD_STAG.pctl_contact ON pctl_contact.id_stg = pc_contact.subtype_stg													

      WHERE													

      pctl_contact.typecode_stg = (''UserContact'')  AND 													

       pc_contact.publicid_stg not in (''default_data:1'',													

             ''systemTables:1'',''systemTables:2'') 													

       AND ((pc_contact.UpdateTime_stg>(:start_dttm) 													

      AND pc_contact.UpdateTime_stg <= (:end_dttm)) 													

      OR (pc_user.UpdateTime_stg>(:start_dttm) 													

      AND pc_user.UpdateTime_stg <= (:end_dttm)))													

UNION													

													

SELECT DISTINCT 													

      ''ClaimCenter'' AS Source,													

      cast(cc_contact.PublicID_stg as varchar(64)) AS publicid, 													

      cast(cc_contact.AddressBookUID_stg as varchar(64)) AS LinkID, 													

      ''SRC_SYS6'' AS SYS_SRC_CD,													

      cctl_contact.NAME_stg AS TL_CNT_Name,													

      (:start_dttm) as start_dttm,													

      (:end_dttm) as end_dttm													

FROM													

      DB_T_PROD_STAG.cc_contact 													

      LEFT OUTER JOIN DB_T_PROD_STAG.cctl_contact ON cctl_contact.id_stg = cc_contact.subtype_stg													

      LEFT OUTER JOIN DB_T_PROD_STAG.cc_user ON cc_user.ContactID_stg = cc_contact.id_stg													

      WHERE													

      (cc_contact.UpdateTime_stg>(:start_dttm) AND cc_contact.UpdateTime_stg <= (:end_dttm) )  or 													

       (cc_user.UpdateTime_stg>(:start_dttm) AND cc_user.UpdateTime_stg <=(:end_dttm) )													

UNION													

													

SELECT 													

      ''ContactManager'' AS Source,													

      CAST(NULL AS VARCHAR(64)) AS PublicID_stg,													

      cast(ab_abcontact.LinkID_stg as varchar(64)) AS LinkID,													

      ''SRC_SYS7'' AS SYS_SRC_CD,													

      abtl_abcontact.NAME_stg AS TL_CNT_Name,													

      (:start_dttm) as start_dttm,													

      (:end_dttm) as end_dttm													

FROM													

     DB_T_PROD_STAG.ab_abcontact													

      LEFT OUTER JOIN DB_T_PROD_STAG.abtl_abcontact ON abtl_abcontact.id_stg = ab_abcontact.subtype_stg													

      /*JOIN (select  contact.AddressBookUID AS AddressBookUID,													

producer_cd.Code AS Code  from  DB_T_PROD_STAG.pc_producercode producer_cd													

join DB_T_PROD_STAG.pc_userproducercode user_producer on (user_producer.ProducerCodeID_stg = producer_cd.id_stg)													

join DB_T_PROD_STAG.pc_user  user1 on(user1.id_stg= user_producer.UserID_stg)													

join DB_T_PROD_STAG.pc_contact contact on(contact.id_stg = user1.ContactID_stg))  C ON AddressBookUID=LinkID*/													

      WHERE													

      ab_abcontact.UpdateTime_stg>(:start_dttm) AND ab_abcontact.UpdateTime_stg <= (:end_dttm)) ab_abcontact													

WHERE  													

      ab_abcontact.TL_CNT_Name IN (''Person'',''Adjudicator'',''User Contact'',''Vendor (Person)'',''Attorney'', ''Doctor'',''Policy Person'',''Contact'',''Lodging (Person)'') AND													

      ((SOURCE = ''ClaimCenter'' AND PublicID IS NOT NULL) OR (SOURCE = ''ContactManager'' AND LinkID IS NOT NULL))													

      union													

      select   DISTINCT													

      CAST(''BUSN'' AS VARCHAR(50)) AS PRTY_TYPE,													

      CAST(NULL AS VARCHAR(50)) AS SOURCE,													

      CAST(NULL AS VARCHAR(50)) AS LinkID,													

     CAST(NULL AS VARCHAR(50)) AS PublicID,													

      SRC_IDNTFTN_VAL,													

      typecode,													

      CAST(NULL AS VARCHAR(50)) AS INTRNL_ORG_TYPE_CD,													

     CAST(NULL AS VARCHAR(50)) AS INTRNL_ORG_SBTYPE_CD,													

      CAST(NULL AS VARCHAR(50)) AS INTRNL_ORG_NUM,													

      SRC_SYS_CD,													

      CAST(2 AS INTEGER) AS sort													

      from (													

      SELECT DISTINCT CAST(NULL AS VARCHAR(50)) AS SOURCE,													

      CASE WHEN (MortgageeLienHolderNumber_alfa_stg IS NOT NULL AND ab_abcontact.LinkID NOT LIKE ''%MORT%''AND ab_abcontact.LinkID NOT LIKE ''%IRS%''  AND  ab_abcontact.Source = ''ContactManager'' )THEN MortgageeLienHolderNumber_alfa_stg 													

       WHEN (   ab_abcontact.Source = ''ContactManager'' )THEN  ab_abcontact.LinkID													

      WHEN ab_abcontact.Source = ''ClaimCenter'' THEN ab_abcontact.PublicID													

      ELSE ''UNK'' END AS TYPECODE,													

/*  ''DBA'' AS name_type_cd,													 */
/*  ab_abcontact.name AS org_name,													 */
      TL_CNT_Name AS SRC_IDNTFTN_VAL,													

      ''SRC_SYS7'' as SRC_SYS_CD													

      													

      													

from     (SELECT 													

      cast(''ClaimCenter'' as varchar(50))AS Source,													

      cast('''' as varchar(250))MortgageeLienHolderNumber_alfa_stg,													

      cast(bc_contact.PublicID_stg as varchar(64)) AS PublicID, 													

      cast(bc_contact.AddressBookUID_stg as varchar(64)) AS LinkID, 													

      ''SRC_SYS5'' AS SYS_SRC_CD,													

      bctl_contact.NAME_stg AS TL_CNT_Name,													

      (:start_dttm) as start_dttm,													

      (:end_dttm) as end_dttm													

FROM													

      DB_T_PROD_STAG.bc_contact 													

      LEFT OUTER JOIN DB_T_PROD_STAG.bc_user ON bc_user.ContactID_stg = bc_contact.id_stg													

      LEFT OUTER JOIN DB_T_PROD_STAG.bctl_contact ON bctl_contact.id_stg = bc_contact.subtype_stg													

      WHERE													

      bctl_contact.typecode_stg = (''UserContact'')  AND 													

    bc_contact.publicid_stg not in (''default_data:1'', ''systemTables:1'',''systemTables:2'') 													

      and ((bc_contact.UpdateTime_stg>(:start_dttm) AND bc_contact.UpdateTime_stg <=(:end_dttm)) OR (bc_user.UpdateTime_stg>(:start_dttm) AND bc_user.UpdateTime_stg <= (:end_dttm)))													

UNION													

													

SELECT     													

      ''ClaimCenter'' AS Source,													

      cast('''' as varchar(250))MortgageeLienHolderNumber_alfa_stg,													

      cast(case 													

             when(bc_contact.ExternalID_stg is not null) then bc_contact.ExternalID_stg 													

             else bc_contact.PublicID_stg 													

             end  as varchar(64))AS PublicID, 													

      cast(bc_contact.AddressBookUID_stg  as varchar(64)) AS LinkID, 													

      ''SRC_SYS5'' AS SYS_SRC_CD,													

      bctl_contact.NAME_stg AS TL_CNT_Name,													

      (:start_dttm) as start_dttm,													

      (:end_dttm) as end_dttm													

from     DB_T_PROD_STAG.bc_account a													

inner join DB_T_PROD_STAG.bc_accountcontact h on h.AccountID_stg = a.id_stg													

inner join DB_T_PROD_STAG.bc_contact on bc_contact.id_stg = h.ContactID_stg													

join DB_T_PROD_STAG.bctl_contact on bctl_contact.ID_stg=bc_contact.Subtype_stg													

left join DB_T_PROD_STAG.bc_accountcontactrole i on i.AccountContactID_stg = h.id_stg													

left join DB_T_PROD_STAG.bctl_accountrole j on j.id_stg = i.Role_stg													

where   ((h.PrimaryPayer_stg = 1) 													

      or (j.name_stg = ''Payer''))													

      and ((bc_contact.UpdateTime_stg>(:start_dttm) 

      AND bc_contact.UpdateTime_stg <=(:end_dttm)) 

          OR (a.UpdateTime_stg>(:start_dttm) 

      AND a.UpdateTime_stg <= (:end_dttm)))

     													

UNION													

													

SELECT 													

      ''ClaimCenter'' AS Source,													

      cast('''' as varchar(250))MortgageeLienHolderNumber_alfa_stg,													

      cast(case when (bc_contact.ExternalID_stg is null) then bc_contact.PublicID_stg else bc_contact.ExternalID_stg end as varchar(64)) AS PublicID, 													

      cast(bc_contact.AddressBookUID_stg as varchar(64)) AS LinkID, 													

      ''SRC_SYS5'' AS SYS_SRC_CD,													

      bctl_contact.NAME_stg AS TL_CNT_Name,													

      (:start_dttm) as start_dttm,													

      (:end_dttm) as end_dttm													

from DB_T_PROD_STAG.bc_account a													

inner join DB_T_PROD_STAG.bc_invoicestream b on a.id_stg = b.AccountID_stg													

inner join DB_T_PROD_STAG.bc_accountcontact c on c.accountid_stg=a.id_stg													

inner join DB_T_PROD_STAG.bc_contact on bc_contact.id_stg = c.ContactID_stg													

join DB_T_PROD_STAG.bctl_contact on bctl_contact.ID_stg=bc_contact.Subtype_stg													

left join DB_T_PROD_STAG.bc_accountcontactrole f on f.AccountContactID_stg = c.id_stg													

left join DB_T_PROD_STAG.bctl_accountrole g on g.id_stg = f.Role_stg													

where ((b.OverridingPayer_alfa_stg is null and c.PrimaryPayer_stg = 1) or (b.OverridingPayer_alfa_stg is not null))													

and ((bc_contact.UpdateTime_stg>(:start_dttm) 

    AND bc_contact.UpdateTime_stg <=(:end_dttm))

          OR (a.UpdateTime_stg>(:start_dttm) 

      AND a.UpdateTime_stg <= (:end_dttm)))

UNION													

													

SELECT     													

''ClaimCenter'' AS Source,													

cast('''' as varchar(250))MortgageeLienHolderNumber_alfa_stg,													

pc_contact.PublicID_stg As publicid,													

cast(Pc_contact.AddressBookUID_stg as varchar(64)) AS LinkID, 													

''SRC_SYS4'' AS SYS_SRC_CD,													

pctl_contact.NAME_stg AS TL_CNT_Name,													

(:start_dttm) as start_dttm,													

(:end_dttm) as end_dttm													

FROM													

      DB_T_PROD_STAG.pc_contact 													

      LEFT OUTER JOIN DB_T_PROD_STAG.pc_user ON pc_user.ContactID_stg = pc_contact.id_stg													

      LEFT OUTER JOIN DB_T_PROD_STAG.pctl_contact ON pctl_contact.id_stg = pc_contact.subtype_stg													

      WHERE													

      pctl_contact.typecode_stg = (''UserContact'')  AND 													

       pc_contact.publicid_stg not in (''default_data:1'',													

             ''systemTables:1'',''systemTables:2'') 													

       AND ((pc_contact.UpdateTime_stg>(:start_dttm) 													

      AND pc_contact.UpdateTime_stg <= (:end_dttm)) 													

      OR (pc_user.UpdateTime_stg>(:start_dttm) 													

      AND pc_user.UpdateTime_stg <= (:end_dttm)))													

UNION													

													

SELECT DISTINCT 													

      ''ClaimCenter'' AS Source,													

      cast('''' as varchar(250))MortgageeLienHolderNumber_alfa_stg,													

      cast(cc_contact.PublicID_stg as varchar(64)) AS publicid, 													

      cast(cc_contact.AddressBookUID_stg as varchar(64)) AS LinkID, 													

      ''SRC_SYS6'' AS SYS_SRC_CD,													

      cctl_contact.NAME_stg AS TL_CNT_Name,													

      (:start_dttm) as start_dttm,													

      (:end_dttm) as end_dttm													

FROM													

      DB_T_PROD_STAG.cc_contact 													

      LEFT OUTER JOIN DB_T_PROD_STAG.cctl_contact ON cctl_contact.id_stg = cc_contact.subtype_stg													

      LEFT OUTER JOIN DB_T_PROD_STAG.cc_user ON cc_user.ContactID_stg = cc_contact.id_stg													

      WHERE													

      (cc_contact.UpdateTime_stg>(:start_dttm) AND cc_contact.UpdateTime_stg <= (:end_dttm) )  or 													

       (cc_user.UpdateTime_stg>(:start_dttm) AND cc_user.UpdateTime_stg <=(:end_dttm) )													

UNION													

													

SELECT 													

      ''ContactManager'' AS Source,													

      MortgageeLienHolderNumber_alfa_stg													

      ,													

      CAST(NULL AS VARCHAR(64)) AS PublicID_stg,													

      cast(ab_abcontact.LinkID_stg as varchar(64)) AS LinkID,													

      ''SRC_SYS7'' AS SYS_SRC_CD,													

      abtl_abcontact.NAME_stg AS TL_CNT_Name,													

      (:start_dttm) as start_dttm,													

      (:end_dttm) as end_dttm													

FROM													

     DB_T_PROD_STAG.ab_abcontact													

      LEFT OUTER JOIN DB_T_PROD_STAG.abtl_abcontact ON abtl_abcontact.id_stg = ab_abcontact.subtype_stg													

      /*JOIN (select  contact.AddressBookUID AS AddressBookUID,													

producer_cd.Code AS Code  from  DB_T_PROD_STAG.pc_producercode producer_cd													

join DB_T_PROD_STAG.pc_userproducercode user_producer on (user_producer.ProducerCodeID_stg = producer_cd.id_stg)													

join DB_T_PROD_STAG.pc_user  user1 on(user1.id_stg= user_producer.UserID_stg)													

join DB_T_PROD_STAG.pc_contact contact on(contact.id_stg = user1.ContactID_stg))  C ON AddressBookUID=LinkID*/													

      WHERE													

      ab_abcontact.UpdateTime_stg>(:start_dttm) AND ab_abcontact.UpdateTime_stg <= (:end_dttm)													

      													

)ab_abcontact/* )DB_T_PROD_STAG.ab_abcontact 													 */
WHERE													

TL_CNT_Name in (''Company'',													

             ''Vendor (Company)'',''Auto Repair Shop'',''Auto Towing Agcy'',''Law Firm'',													

             ''Medical Care Organization'',''Lodging (Company)'',''Lodging Provider (Org)'')													

													

union													

													

													

SELECT     DISTINCT CAST(NULL AS VARCHAR(50)) AS SOURCE,													

      UPPER(cctl_salvageyard_alfa.TYPECODE_stg) AS TYPECODE, 													

      ''SALVG'' AS SRC_IDNTFTN_VAL,													

      ''SRC_SYS6'' as SRC_SYS_CD													

FROM													

      (													

      SELECT     DISTINCT  cc_incident.SalvageYard_alfa_stg,cc_incident.UpdateTime_stg													

      FROM													

      DB_T_PROD_STAG.cc_incident 													

      WHERE      													

      cc_incident.UpdateTime_stg > (:start_dttm)													

             and cc_incident.UpdateTime_stg <= (:end_dttm)) cc_incident 													

      join DB_T_PROD_STAG.cctl_salvageyard_alfa    													

      on cc_incident.SalvageYard_alfa_stg=cctl_salvageyard_alfa.ID_stg													

QUALIFY    ROW_NUMBER () OVER (													

PARTITION BY cctl_salvageyard_alfa.TYPECODE_stg 													

ORDER BY cc_incident.updatetime_stg DESC )=1													

													

UNION 													

													

SELECT    DISTINCT CAST(NULL AS VARCHAR(50)) AS SOURCE,													

     UPPER(pctl_priorcarrier_alfa.TYPECODE_stg) AS TYPECODE, 													

      ''Insurance Carrier'' AS SRC_IDNTFTN_VAL,													

      ''SRC_SYS4'' as SRC_SYS_CD													

FROM													

     DB_T_PROD_STAG.pctl_priorcarrier_alfa   													

left outer join (													

      SELECT     distinct effdt.PriorCarrier_alfa_stg,effdt.UpdateTime_stg 													

      from      DB_T_PROD_STAG.pc_effectivedatedfields as effdt													

      left outer join DB_T_PROD_STAG.pc_policyperiod pp 													

             on pp.id_stg=effdt.BranchID_stg													

      LEFT OUTER JOIN DB_T_PROD_STAG.pc_job pcj 													

             ON pcj.id_stg = pp.JobID_stg													

      LEFT OUTER  JOIN DB_T_PROD_STAG.pctl_job pctlj 													

             ON pctlj.id_stg=pcj.Subtype_stg													

      LEFT JOIN DB_T_PROD_STAG.pctl_policyperiodstatus pps 													

             ON pps.id_stg = pp.Status_stg													

      WHERE      effdt.ExpirationDate_stg is null 													

             and pctlj.TYPECODE_stg  IN (''Cancellation'',''PolicyChange'',''Reinstatement'',													

                   ''Renewal'',''Rewrite'',''Submission'')													

      AND pps.Typecode_stg<>''Temporary'' 													

             /*and effdt.UpdateTime_stg > (:start_dttm) 													

             and effdt.UpdateTime_stg <= (:end_dttm)*/) pc_effectivedatedfields  													

      on pc_effectivedatedfields.PriorCarrier_alfa_stg=pctl_priorcarrier_alfa.id_stg)A													

      													

      union													

      select 													

CAST(''INTRNL_ORG'' AS VARCHAR(50)) AS PRTY_TYPE,													

     CAST(NULL AS VARCHAR(50)) AS SOURCE,													

      CAST(NULL AS VARCHAR(50)) AS LinkID,													

         CAST(NULL AS VARCHAR(50)) AS PublicID,													

      CAST(NULL AS VARCHAR(50)) AS BUSN_CTGY_CD,													

      CAST(NULL AS VARCHAR(50)) AS NK_BUSN_CD,													

      "Type",													

     "Subtype",													

     "Key",													

      SYS_SRC_CD,													

      CAST(3 AS INTEGER) AS sort													

from (													

SELECT     													

      TYPECODE_stg AS "Key", 													

      CAST(''INTRNL_ORG_TYPE15'' AS VARCHAR(50)) AS "Type",													

      CAST( ''INTRNL_ORG_SBTYPE1''  AS VARCHAR ( 50)) AS "Subtype", 													

      ''SRC_SYS4'' AS SYS_SRC_CD													

FROM  DB_T_PROD_STAG.pctl_uwcompanycode													

													

UNION 													

													

/*SELECT     													

      CAST(pc_group.id_stg AS VARCHAR(50)) AS "KEY" , 													

      ''INTRNL_ORG_TYPE15'' AS "Type", 													

      pctl_grouptype.typecode_stg AS "Subtype",													

      ''SRC_SYS4'' AS SYS_SRC_CD													

FROM  													

      (SELECT  pc_group.ID_stg,pc_group.GroupType_stg													

FROM													

DB_T_PROD_STAG.pc_group 													

WHERE 													

pc_group.UpdateTime_stg > (:start_dttm)													

      and pc_group.UpdateTime_stg <= (:end_dttm)													

      )DB_T_PROD_STAG.pc_group 													

      JOIN DB_T_PROD_STAG.pctl_grouptype ON    pc_group.GroupType_stg = pctl_grouptype.ID_stg													

WHERE      													

      pctl_grouptype.TYPECODE_stg = ''servicecenter_alfa''													

UNION*/													

SELECT     													

      Code_stg AS "Key", 													

      CAST(''INTRNL_ORG_TYPE15'' AS VARCHAR(50)) AS "Type",													

      CAST(''INTRNL_ORG_SBTYPE2'' AS VARCHAR(50)) AS "Subtype", 													

      ''SRC_SYS4'' AS SYS_SRC_CD													

FROM  (SELECT  pc_producercode.Code_stg 													

FROM													

  DB_T_PROD_STAG.pc_producercode 													

 join DB_T_PROD_STAG.pc_userproducercode upc on upc.ProducerCodeID_stg = pc_producercode.ID_stg													

join DB_T_PROD_STAG.pc_user usr on usr.id_stg = upc.UserID_stg													

join DB_T_PROD_STAG.pc_contact cnt on cnt.id_stg = usr.ContactID_stg   WHERE													

pc_producercode.UpdateTime_stg > (:start_dttm) AND pc_producercode.UpdateTime_stg <= (:end_dttm)) pc_producercode 													

 UNION													

SELECT    													

      cc_group.name_stg AS "Key", 													

      CAST( ''INTRNL_ORG_TYPE15'' AS VARCHAR(50)) AS "Type",													

      cctl_grouptype.typecode_stg AS "Subtype", 													

      ''SRC_SYS6'' AS SYS_SRC_CD													

FROM  													

      (SELECT  cc_group.Name_stg,  cc_group.GroupType_stg													

FROM													

DB_T_PROD_STAG.CC_GROUP													

where 													

cc_group.UpdateTime_stg > (:start_dttm)													

and cc_group.UpdateTime_stg <= (:end_dttm)) CC_GROUP, 													

      DB_T_PROD_STAG.CCTL_GROUPTYPE													

WHERE      													

      CC_GROUP.GroupType_stg = CCTL_GROUPTYPE.ID_stg													

UNION													

SELECT     													

      pc_group.name_stg AS "Key", 													

      CAST(''INTRNL_ORG_TYPE15'' AS VARCHAR(50)) AS "Type",													

      c.typecode_stg AS "Subtype", 													

      ''SRC_SYS4'' AS SYS_SRC_CD													

FROM  													

      (SELECT  pc_group.Name_stg,pc_group.GroupType_stg,pc_group.Contact_alfa_stg													

FROM													

DB_T_PROD_STAG.pc_group left join (select id_stg from DB_T_PROD_STAG.pc_contact) c on c.id_stg = pc_group.Contact_alfa_stg													

WHERE 													

pc_group.UpdateTime_stg > (:start_dttm)													

      and pc_group.UpdateTime_stg <= (:end_dttm)) pc_group 													

      INNER JOIN (select typecode_stg,id_stg from  DB_T_PROD_STAG.pctl_grouptype) C ON C.id_stg=pc_group.GroupType_stg													

WHERE      													

      c.TYPECODE_stg IN (''root'',''underwritingdistrict_alfa'',''homeofficeuw'')													

UNION													

SELECT     													

      DISTINCT name_stg AS "Key", 													

      CAST(''INTRNL_ORG_TYPE15'' AS VARCHAR(50)) AS "Type",													

      CAST(''INTRNL_ORG_SBTYPE3'' AS VARCHAR(50)) AS "Subtype", 													

      ''SRC_SYS4'' AS SYS_SRC_CD      													

FROM  													

      (SELECT pc_region.Name_stg													

FROM													

DB_T_PROD_STAG.pc_region													

where													

pc_region.UpdateTime_stg > (:start_dttm)													

and pc_region.UpdateTime_stg <= (:end_dttm)) pc_region 													

UNION													

SELECT     													

      DISTINCT pc_region_zone.code_stg AS "Key",													

      CAST(''INTRNL_ORG_TYPE15'' AS VARCHAR(50)) AS "Type", 													

      CAST(''INTRNL_ORG_SBTYPE5'' AS VARCHAR(50)) AS "Subtype",													

      ''SRC_SYS4'' AS SYS_SRC_CD													

FROM  													

      (SELECT  pc_region_zone.ZoneType_stg,  pc_region_zone.Code_stg													

FROM DB_T_PROD_STAG.pc_region_zone) pc_region_zone 													

      INNER JOIN (select TYPECODE_stg,id_stg from  DB_T_PROD_STAG.pctl_zonetype) C ON pc_region_zone.ZoneType_stg=C.id_stg													

WHERE      													

      C.TYPECODE_stg=''state'' 													

UNION													

SELECT     													

      pc_group.name_stg AS "Key", 													

      CAST(''INTRNL_ORG_TYPE15'' AS VARCHAR(50)) AS "Type",													

      c.typecode_stg AS "Subtype",													

      ''SRC_SYS4'' AS SYS_SRC_CD													

FROM  													

      (SELECT  pc_group.Name_stg, pc_group.GroupType_stg													

FROM													

DB_T_PROD_STAG.pc_group left join (SELECT  pc_contact.ID_stg													

FROM													

DB_T_PROD_STAG.pc_contact													

WHERE pc_contact.UpdateTime_stg> (:start_dttm)													

and pc_contact.UpdateTime_stg <= (:end_dttm)) c on c.id_stg = pc_group.Contact_alfa_stg													

WHERE 													

pc_group.UpdateTime_stg > (:start_dttm)													

      and pc_group.UpdateTime_stg <= (:end_dttm)) pc_group 													

      INNER JOIN (select typecode_stg,id_stg from DB_T_PROD_STAG.pctl_grouptype) C ON c.id_stg=pc_group.GroupType_stg													

WHERE      													

      c.TYPECODE_stg IN (''region'',''salesdistrict_alfa'',''servicecenter_alfa'')													

UNION													

SELECT     													

      DISTINCT pc_region_zone.code_stg AS "Key",													

      CAST(''INTRNL_ORG_TYPE15'' AS VARCHAR(50)) AS "Type", 													

      CAST(''INTRNL_ORG_SBTYPE4'' AS VARCHAR(50)) AS "Subtype",													

      ''SRC_SYS4'' AS SYS_SRC_CD													

FROM  (SELECT  pc_region_zone.ZoneType_stg, pc_region_zone.Code_stg													

FROM													

DB_T_PROD_STAG.pc_region_zone) pc_region_zone 													

INNER JOIN (select id_stg,TYPECODE_stg from DB_T_PROD_STAG.pctl_zonetype) C ON   pc_region_zone.ZoneType_stg=C.id_stg													

WHERE      c.TYPECODE_stg=''state'' ) a													

ORDER BY 11 ASC
) SRC
)
);


-- Component exp_pass_to_target, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_target AS
(
SELECT distinct
sq_xref_prty.PRTY_TYPE as PRTY_TYPE,
sq_xref_prty.INDIV_SRC_SYS as INDIV_SRC_SYS,
sq_xref_prty.NK_LINK_ID as NK_LINK_ID,
sq_xref_prty.NK_PUBLC_ID as NK_PUBLC_ID,
CASE
  WHEN sq_xref_prty.BUSN_CTGY_CD = ''Insurance Carrier'' THEN LKP_1.TGT_IDNTFTN_VAL
  WHEN sq_xref_prty.BUSN_CTGY_CD = ''SALVG'' THEN LKP_2.TGT_IDNTFTN_VAL
  WHEN sq_xref_prty.BUSN_CTGY_CD NOT IN (''SALVG'', ''Insurance Carrier'') THEN LKP_3.TGT_IDNTFTN_VAL
  ELSE ''UNK''
END AS var_BUSN_CTGY_CD,
var_BUSN_CTGY_CD as out_BUSN_CTGY_CD,
sq_xref_prty.NK_BUSN_CD as NK_BUSN_CD,
LKP_4.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_INTRNL_ORG_TYPE */ as var_INTRNL_ORG_TYPE_CD,
var_INTRNL_ORG_TYPE_CD as out_INTRNL_ORG_TYPE_CD,
LKP_5.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_INTRNL_ORG_SBTYPE */ as var_INTRNL_ORG_SBTYPE_CD,
var_INTRNL_ORG_SBTYPE_CD as out_INTRNL_ORG_SBTYPE_CD,
sq_xref_prty.INTRNL_ORG_NUM as INTRNL_ORG_NUM,
LKP_6.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SRC_CD */ as var_SRC_SYS_CD,
var_SRC_SYS_CD as out_SRC_SYS_CD,
CURRENT_TIMESTAMP as LOAD_DTTM,
DECODE ( TRUE , LKP_7.PRTY_ID /* replaced lookup LKP_XREF_PRTY */ IS NULL , ''I'' , ''R'' ) as ins_rej_flg/*,
sq_xref_prty.source_record_id,
row_number() over (partition by sq_xref_prty.source_record_id order by sq_xref_prty.source_record_id) as RNK*/
FROM
sq_xref_prty
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = ''BUSN_CTGY6''
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = ''BUSN_CTGY5''
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT LKP_3 ON LKP_3.SRC_IDNTFTN_VAL = sq_xref_prty.BUSN_CTGY_CD
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_INTRNL_ORG_TYPE LKP_4 ON LKP_4.SRC_IDNTFTN_VAL = sq_xref_prty.INTRNL_ORG_TYPE_CD
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_INTRNL_ORG_SBTYPE LKP_5 ON LKP_5.SRC_IDNTFTN_VAL = sq_xref_prty.INTRNL_ORG_SBTYPE_CD
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_SRC_CD LKP_6 ON LKP_6.SRC_IDNTFTN_VAL = sq_xref_prty.SRC_SYS_CD
/*LEFT JOIN LKP_XREF_PRTY LKP_7 ON LKP_7.DIR_PRTY_VAL = sq_xref_prty.PRTY_TYPE AND LKP_7.INDIV_SRC_VAL = sq_xref_prty.INDIV_SRC_SYS AND LKP_7.NK_LNK_ID = sq_xref_prty.NK_LINK_ID AND LKP_7.NK_PUBLC_ID = sq_xref_prty.NK_PUBLC_ID AND LKP_7.BUSN_CTGY_CD = var_BUSN_CTGY_CD AND LKP_7.NK_BUSN_VAL = sq_xref_prty.NK_BUSN_CD AND LKP_7.INTRNL_ORG_TYPE_CD = var_INTRNL_ORG_TYPE_CD AND LKP_7.INTRNL_ORG_SBTYPE_CD = var_INTRNL_ORG_SBTYPE_CD AND LKP_7.INTRNL_ORG_NUM = sq_xref_prty.INTRNL_ORG_NUM AND LKP_7.SRC_SYS_CD = var_SRC_SYS_CD
QUALIFY RNK = 1*/
LEFT JOIN LKP_XREF_PRTY LKP_7 ON coalesce(LKP_7.DIR_PRTY_VAL,'') = coalesce(sq_xref_prty.PRTY_TYPE ,'')
	AND coalesce(LKP_7.INDIV_SRC_VAL,'') = coalesce(sq_xref_prty.INDIV_SRC_SYS ,'')
	AND coalesce(LKP_7.NK_LNK_ID,'') = coalesce(sq_xref_prty.NK_LINK_ID ,'')
	AND coalesce(LKP_7.NK_PUBLC_ID,'') = coalesce(sq_xref_prty.NK_PUBLC_ID ,'')
	AND coalesce(LKP_7.BUSN_CTGY_CD,'') = coalesce(var_BUSN_CTGY_CD ,'')
	AND coalesce(LKP_7.NK_BUSN_VAL,'') = coalesce(sq_xref_prty.NK_BUSN_CD ,'')
	AND coalesce(LKP_7.INTRNL_ORG_TYPE_CD ,'')= coalesce(var_INTRNL_ORG_TYPE_CD ,'')
	AND coalesce(LKP_7.INTRNL_ORG_SBTYPE_CD,'') =coalesce( var_INTRNL_ORG_SBTYPE_CD ,'')
	AND coalesce(LKP_7.INTRNL_ORG_NUM,'') = coalesce(sq_xref_prty.INTRNL_ORG_NUM,'') 
	AND coalesce(LKP_7.SRC_SYS_CD ,'')= coalesce(var_SRC_SYS_CD,'')--LEFT JOIN LKP_XREF_PRTY LKP_7 ON LKP_7.DIR_PRTY_VAL = sq_xref_prty.PRTY_TYPE AND LKP_7.INDIV_SRC_VAL = sq_xref_prty.INDIV_SRC_SYS AND LKP_7.NK_LNK_ID = sq_xref_prty.NK_LINK_ID AND LKP_7.NK_PUBLC_ID = sq_xref_prty.NK_PUBLC_ID AND LKP_7.BUSN_CTGY_CD = var_BUSN_CTGY_CD AND LKP_7.NK_BUSN_VAL = sq_xref_prty.NK_BUSN_CD AND LKP_7.INTRNL_ORG_TYPE_CD = var_INTRNL_ORG_TYPE_CD AND LKP_7.INTRNL_ORG_SBTYPE_CD = var_INTRNL_ORG_SBTYPE_CD AND LKP_7.INTRNL_ORG_NUM = sq_xref_prty.INTRNL_ORG_NUM AND LKP_7.SRC_SYS_CD = var_SRC_SYS_CD
--QUALIFY RNK = 1
);


-- Component flt_ins_rej_prty_id, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE flt_ins_rej_prty_id AS
(
SELECT
exp_pass_to_target.PRTY_TYPE as PRTY_TYPE,
exp_pass_to_target.INDIV_SRC_SYS as INDIV_SRC_SYS,
exp_pass_to_target.NK_LINK_ID as NK_LINK_ID,
exp_pass_to_target.NK_PUBLC_ID as NK_PUBLC_ID,
exp_pass_to_target.out_BUSN_CTGY_CD as BUSN_CTGY_CD,
exp_pass_to_target.NK_BUSN_CD as NK_BUSN_CD,
exp_pass_to_target.out_INTRNL_ORG_TYPE_CD as INTRNL_ORG_TYPE_CD,
exp_pass_to_target.out_INTRNL_ORG_SBTYPE_CD as INTRNL_ORG_SBTYPE_CD,
exp_pass_to_target.INTRNL_ORG_NUM as INTRNL_ORG_NUM,
exp_pass_to_target.out_SRC_SYS_CD as SRC_SYS_CD,
exp_pass_to_target.LOAD_DTTM as LOAD_DTTM,
exp_pass_to_target.ins_rej_flg as ins_rej_flg/*,
exp_pass_to_target.source_record_id*/
FROM
exp_pass_to_target
WHERE exp_pass_to_target.ins_rej_flg = ''I''
);


-- Component DIR_PRTY, Type TARGET 
INSERT INTO DB_T_PROD_CORE.DIR_PRTY
(
PRTY_ID,
DIR_PRTY_VAL,
INDIV_SRC_VAL,
NK_LNK_ID,
NK_PUBLC_ID,
BUSN_CTGY_CD,
NK_BUSN_VAL,
INTRNL_ORG_TYPE_CD,
INTRNL_ORG_SBTYPE_CD,
INTRNL_ORG_NUM,
SRC_SYS_CD,
LOAD_DTTM
)
SELECT
public.seq_prty_id.nextval as PRTY_ID,
flt_ins_rej_prty_id.PRTY_TYPE as DIR_PRTY_VAL,
flt_ins_rej_prty_id.INDIV_SRC_SYS as INDIV_SRC_VAL,
flt_ins_rej_prty_id.NK_LINK_ID as NK_LNK_ID,
flt_ins_rej_prty_id.NK_PUBLC_ID as NK_PUBLC_ID,
flt_ins_rej_prty_id.BUSN_CTGY_CD as BUSN_CTGY_CD,
flt_ins_rej_prty_id.NK_BUSN_CD as NK_BUSN_VAL,
flt_ins_rej_prty_id.INTRNL_ORG_TYPE_CD as INTRNL_ORG_TYPE_CD,
flt_ins_rej_prty_id.INTRNL_ORG_SBTYPE_CD as INTRNL_ORG_SBTYPE_CD,
flt_ins_rej_prty_id.INTRNL_ORG_NUM as INTRNL_ORG_NUM,
flt_ins_rej_prty_id.SRC_SYS_CD as SRC_SYS_CD,
flt_ins_rej_prty_id.LOAD_DTTM as LOAD_DTTM
FROM
flt_ins_rej_prty_id;


END; 
';