-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_TLPHN_NUM_INSUPD("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '  
DECLARE start_dttm TIMESTAMP;
end_dttm TIMESTAMP;
PRCS_ID INTEGER;
NEXTVAL INTEGER;
BEGIN 
start_dttm := CURRENT_TIMESTAMP();
end_dttm := CURRENT_TIMESTAMP();
PRCS_ID := 1;  
NEXTVAL :=1;

-- Component LKP_TERADATA_ETL_REF_XLAT_ADDRESS_SBTYPE, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_ADDRESS_SBTYPE AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''ADDR_SBTYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_LOCTR_SBTYPE, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_LOCTR_SBTYPE AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''LOCTR_SBTYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TLPHN_NUM, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TLPHN_NUM AS
(
SELECT
TLPHN_NUM_ID,
TLPHN_NUM
FROM DB_T_PROD_CORE.TLPHN_NUM
);


-- Component sq_pc_contact, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_pc_contact AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as Phone,
$2 as Retired,
$3 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select A.phone,A.Retired  FROM 

(

/*  HomePhone  */
SELECT ab_abcontact.HomePhone as phone,

Retired as Retired,createtime as createtime

FROM

(SELECT 

bc_contact.HomePhone_stg HomePhone, /*  01 */
bc_contact.CellPhone_stg CellPhone, /*  02  */
bc_contact.WorkPhone_stg WorkPhone, /* 03 */
bc_contact.PrimaryPhone_stg PrimaryPhone, /* 04 */
bc_contact.Retired_stg Retired, /* 05 */
bc_contact.CreateTime_stg CreateTime/* 06 */
FROM

	DB_T_PROD_STAG.bc_contact 

	LEFT OUTER JOIN DB_T_PROD_STAG.bctl_contact ON bctl_contact.id_stg = bc_contact.subtype_stg 

	LEFT OUTER JOIN DB_T_PROD_STAG.bc_user ON bc_user.ContactID_stg = bc_contact.id_stg

WHERE

	bctl_contact.typecode_stg = (''UserContact'')  AND 

/*  below condition added to avoid duplicates */
    bc_contact.publicid_stg not in (''default_data:1'', ''systemTables:1'',''systemTables:2'') 

	and ((bc_contact.UpdateTime_stg>(:start_dttm) AND bc_contact.UpdateTime_stg <=(:end_dttm)) OR (bc_user.UpdateTime_stg>(:start_dttm) AND bc_user.UpdateTime_stg <= (:end_dttm)))

	

UNION



/*  Primary and Secondary Payer contact (this is at the Account level) */


SELECT 

bc_contact.HomePhone_stg HomePhone, /* 01 */
bc_contact.CellPhone_stg CellPhone, /* 02  */
bc_contact.WorkPhone_stg WorkPhone, /* 03 */
bc_contact.PrimaryPhone_stg PrimaryPhone, /* 04  */
bc_contact.Retired_stg Retired, /* 05  */
bc_contact.CreateTime_stg CreateTime/* 06  */


from DB_T_PROD_STAG.bc_account a

inner join DB_T_PROD_STAG.bc_accountcontact h on h.AccountID_stg = a.id_stg

inner join DB_T_PROD_STAG.bc_contact on bc_contact.id_stg = h.ContactID_stg

join DB_T_PROD_STAG.bctl_contact on bctl_contact.id_stg=bc_contact.subtype_stg

left join DB_T_PROD_STAG.bc_accountcontactrole i on i.AccountContactID_stg = h.id_stg

left join DB_T_PROD_STAG.bctl_accountrole j on j.id_stg = i.Role_stg

LEFT OUTER JOIN DB_T_PROD_STAG.bc_user ON bc_user.ContactID_stg = bc_contact.id_stg

where ((h.PrimaryPayer_stg = 1) or (j.name_stg = ''Payer''))

and ((bc_contact.UpdateTime_stg>(:start_dttm) AND bc_contact.UpdateTime_stg <=(:end_dttm)) OR (bc_user.UpdateTime_stg>(:start_dttm) AND bc_user.UpdateTime_stg <= (:end_dttm)))



UNION

/*  Primary Payer and Overiding Payer Contact (this is at the Invoicestream level) */


SELECT 

bc_contact.HomePhone_stg HomePhone, /* 01  */
bc_contact.CellPhone_stg CellPhone, /* 02  */
bc_contact.WorkPhone_stg WorkPhone, /* 03  */
bc_contact.PrimaryPhone_stg PrimaryPhone, /* 04  */
bc_contact.Retired_stg Retired, /* 05  */
bc_contact.CreateTime_stg CreateTime/* 06 */
from DB_T_PROD_STAG.bc_account a

inner join DB_T_PROD_STAG.bc_invoicestream b on a.id_stg = b.AccountID_stg

inner join DB_T_PROD_STAG.bc_accountcontact c on c.AccountID_stg=a.id_stg

inner join DB_T_PROD_STAG.bc_contact on bc_contact.id_stg = c.ContactID_stg

join DB_T_PROD_STAG.bctl_contact on bctl_contact.id_stg=bc_contact.subtype_stg

LEFT OUTER JOIN DB_T_PROD_STAG.bc_user ON bc_user.ContactID_stg = bc_contact.id_stg

where ((b.OverridingPayer_alfa_stg is null and c.PrimaryPayer_stg = 1) or (b.OverridingPayer_alfa_stg is not null))

and ((bc_contact.UpdateTime_stg>(:start_dttm) AND bc_contact.UpdateTime_stg <=(:end_dttm)) OR (bc_user.UpdateTime_stg>(:start_dttm) AND bc_user.UpdateTime_stg <= (:end_dttm)))



UNION



SELECT 

pc_contact.HomePhone_stg HomePhone, /* 01  */
pc_contact.CellPhone_stg CellPhone, /* 02  */
pc_contact.WorkPhone_stg WorkPhone, /* 03  */
pc_contact.PrimaryPhone_stg PrimaryPhone, /* 04  */
pc_contact.Retired_stg Retired, /* 05  */
pc_contact.CreateTime_stg CreateTime/* 06  */
FROM DB_T_PROD_STAG.pc_contact 

	LEFT OUTER JOIN DB_T_PROD_STAG.pctl_contact ON pctl_contact.id_stg = pc_contact.subtype_stg 

	LEFT OUTER JOIN DB_T_PROD_STAG.pc_user ON pc_user.ContactID_stg = pc_contact.id_stg

WHERE pctl_contact.typecode_stg = (''UserContact'')  AND 

/*  below condition added to avoid duplicates */
       pc_contact.publicid_stg not in (''default_data:1'', ''systemTables:1'',''systemTables:2'') 

	 AND ((pc_contact.UpdateTime_stg>(:start_dttm) AND pc_contact.UpdateTime_stg <= (:end_dttm)) OR (pc_user.UpdateTime_stg>(:start_dttm) AND pc_user.UpdateTime_stg <= (:end_dttm)))

	 

UNION



SELECT DISTINCT 

cc_contact.HomePhone_stg HomePhone, /* 01  */
cc_contact.CellPhone_stg CellPhone, /* 02  */
cc_contact.WorkPhone_stg WorkPhone, /* 03  */
cc_contact.PrimaryPhone_stg PrimaryPhone, /* 04  */
cc_contact.Retired_stg Retired, /* 05  */
cc_contact.CreateTime_stg CreateTime/* 06  */
FROM

	DB_T_PROD_STAG.cc_contact 

	LEFT OUTER JOIN DB_T_PROD_STAG.cctl_contact ON cctl_contact.id_stg = cc_contact.subtype_stg 

	LEFT OUTER JOIN DB_T_PROD_STAG.cc_user ON cc_user.ContactID_stg = cc_contact.id_stg

WHERE

	(cc_contact.UpdateTime_stg>(:start_dttm) AND cc_contact.UpdateTime_stg <= (:end_dttm) )  or 

       (cc_user.UpdateTime_stg>(:start_dttm) AND cc_user.UpdateTime_stg <=(:end_dttm) )

	   

UNION



SELECT 

ab_abcontact.HomePhone_stg HomePhone, /* 01  */
ab_abcontact.CellPhone_stg CellPhone, /* 02  */
ab_abcontact.WorkPhone_stg WorkPhone, /* 03 */
ab_abcontact.PrimaryPhone_stg PrimaryPhone, /* 04 */
ab_abcontact.Retired_stg Retired, /* 05  */
ab_abcontact.CreateTime_stg CreateTime/* 06 */
FROM

 	DB_T_PROD_STAG.ab_abcontact

	LEFT OUTER JOIN DB_T_PROD_STAG.abtl_abcontact ON abtl_abcontact.id_stg = ab_abcontact.subtype_stg 

	LEFT OUTER JOIN DB_T_PROD_STAG.ab_user ON ab_user.ContactID_stg = ab_abcontact.id_stg

WHERE

ab_abcontact.UpdateTime_stg>(:start_dttm) AND ab_abcontact.UpdateTime_stg <= (:end_dttm))

 ab_abcontact

where Retired=0

/* ------------------------------------------------- */
UNION

/* -------------------------------------------------- */
/*  CellPhone */
SELECT   ab_abcontact.CellPhone as phone,

Retired as Retired,createtime as createtime

FROM

(SELECT 

bc_contact.HomePhone_stg HomePhone, /*  01 */
bc_contact.CellPhone_stg CellPhone, /*  02  */
bc_contact.WorkPhone_stg WorkPhone, /* 03 */
bc_contact.PrimaryPhone_stg PrimaryPhone, /* 04 */
bc_contact.Retired_stg Retired, /* 05 */
bc_contact.CreateTime_stg CreateTime/* 06 */
FROM

	DB_T_PROD_STAG.bc_contact 

	LEFT OUTER JOIN DB_T_PROD_STAG.bctl_contact ON bctl_contact.id_stg = bc_contact.subtype_stg 

	LEFT OUTER JOIN DB_T_PROD_STAG.bc_user ON bc_user.ContactID_stg = bc_contact.id_stg

WHERE

	bctl_contact.typecode_stg = (''UserContact'')  AND 

/*  below condition added to avoid duplicates */
    bc_contact.publicid_stg not in (''default_data:1'', ''systemTables:1'',''systemTables:2'') 

	and ((bc_contact.UpdateTime_stg>(:start_dttm) AND bc_contact.UpdateTime_stg <=(:end_dttm)) OR (bc_user.UpdateTime_stg>(:start_dttm) AND bc_user.UpdateTime_stg <= (:end_dttm)))

	

UNION



/*  Primary and Secondary Payer contact (this is at the Account level) */


SELECT 

bc_contact.HomePhone_stg HomePhone, /* 01 */
bc_contact.CellPhone_stg CellPhone, /* 02  */
bc_contact.WorkPhone_stg WorkPhone, /* 03 */
bc_contact.PrimaryPhone_stg PrimaryPhone, /* 04  */
bc_contact.Retired_stg Retired, /* 05  */
bc_contact.CreateTime_stg CreateTime/* 06  */


from DB_T_PROD_STAG.bc_account a

inner join DB_T_PROD_STAG.bc_accountcontact h on h.AccountID_stg = a.id_stg

inner join DB_T_PROD_STAG.bc_contact on bc_contact.id_stg = h.ContactID_stg

join DB_T_PROD_STAG.bctl_contact on bctl_contact.id_stg=bc_contact.subtype_stg

left join DB_T_PROD_STAG.bc_accountcontactrole i on i.AccountContactID_stg = h.id_stg

left join DB_T_PROD_STAG.bctl_accountrole j on j.id_stg = i.Role_stg

LEFT OUTER JOIN DB_T_PROD_STAG.bc_user ON bc_user.ContactID_stg = bc_contact.id_stg

where ((h.PrimaryPayer_stg = 1) or (j.name_stg = ''Payer''))

and ((bc_contact.UpdateTime_stg>(:start_dttm) AND bc_contact.UpdateTime_stg <=(:end_dttm)) OR (bc_user.UpdateTime_stg>(:start_dttm) AND bc_user.UpdateTime_stg <= (:end_dttm)))



UNION

/*  Primary Payer and Overiding Payer Contact (this is at the Invoicestream level) */


SELECT 

bc_contact.HomePhone_stg HomePhone, /* 01  */
bc_contact.CellPhone_stg CellPhone, /* 02  */
bc_contact.WorkPhone_stg WorkPhone, /* 03  */
bc_contact.PrimaryPhone_stg PrimaryPhone, /* 04  */
bc_contact.Retired_stg Retired, /* 05  */
bc_contact.CreateTime_stg CreateTime/* 06 */
from DB_T_PROD_STAG.bc_account a

inner join DB_T_PROD_STAG.bc_invoicestream b on a.id_stg = b.AccountID_stg

inner join DB_T_PROD_STAG.bc_accountcontact c on c.AccountID_stg=a.id_stg

inner join DB_T_PROD_STAG.bc_contact on bc_contact.id_stg = c.ContactID_stg

join DB_T_PROD_STAG.bctl_contact on bctl_contact.id_stg=bc_contact.subtype_stg

LEFT OUTER JOIN DB_T_PROD_STAG.bc_user ON bc_user.ContactID_stg = bc_contact.id_stg

where ((b.OverridingPayer_alfa_stg is null and c.PrimaryPayer_stg = 1) or (b.OverridingPayer_alfa_stg is not null))

and ((bc_contact.UpdateTime_stg>(:start_dttm) AND bc_contact.UpdateTime_stg <=(:end_dttm)) OR (bc_user.UpdateTime_stg>(:start_dttm) AND bc_user.UpdateTime_stg <= (:end_dttm)))



UNION



SELECT 

pc_contact.HomePhone_stg HomePhone, /* 01  */
pc_contact.CellPhone_stg CellPhone, /* 02  */
pc_contact.WorkPhone_stg WorkPhone, /* 03  */
pc_contact.PrimaryPhone_stg PrimaryPhone, /* 04  */
pc_contact.Retired_stg Retired, /* 05  */
pc_contact.CreateTime_stg CreateTime/* 06  */
FROM DB_T_PROD_STAG.pc_contact 

	LEFT OUTER JOIN DB_T_PROD_STAG.pctl_contact ON pctl_contact.id_stg = pc_contact.subtype_stg 

	LEFT OUTER JOIN DB_T_PROD_STAG.pc_user ON pc_user.ContactID_stg = pc_contact.id_stg

WHERE pctl_contact.typecode_stg = (''UserContact'')  AND 

/*  below condition added to avoid duplicates */
       pc_contact.publicid_stg not in (''default_data:1'', ''systemTables:1'',''systemTables:2'') 

	 AND ((pc_contact.UpdateTime_stg>(:start_dttm) AND pc_contact.UpdateTime_stg <= (:end_dttm)) OR (pc_user.UpdateTime_stg>(:start_dttm) AND pc_user.UpdateTime_stg <= (:end_dttm)))

	 

UNION



SELECT DISTINCT 

cc_contact.HomePhone_stg HomePhone, /* 01  */
cc_contact.CellPhone_stg CellPhone, /* 02  */
cc_contact.WorkPhone_stg WorkPhone, /* 03  */
cc_contact.PrimaryPhone_stg PrimaryPhone, /* 04  */
cc_contact.Retired_stg Retired, /* 05  */
cc_contact.CreateTime_stg CreateTime/* 06  */
FROM

	DB_T_PROD_STAG.cc_contact 

	LEFT OUTER JOIN DB_T_PROD_STAG.cctl_contact ON cctl_contact.id_stg = cc_contact.subtype_stg 

	LEFT OUTER JOIN DB_T_PROD_STAG.cc_user ON cc_user.ContactID_stg = cc_contact.id_stg

WHERE

	(cc_contact.UpdateTime_stg>(:start_dttm) AND cc_contact.UpdateTime_stg <= (:end_dttm) )  or 

       (cc_user.UpdateTime_stg>(:start_dttm) AND cc_user.UpdateTime_stg <=(:end_dttm) )

	   

UNION



SELECT 

ab_abcontact.HomePhone_stg HomePhone, /* 01  */
ab_abcontact.CellPhone_stg CellPhone, /* 02  */
ab_abcontact.WorkPhone_stg WorkPhone, /* 03 */
ab_abcontact.PrimaryPhone_stg PrimaryPhone, /* 04 */
ab_abcontact.Retired_stg Retired, /* 05  */
ab_abcontact.CreateTime_stg CreateTime/* 06 */
FROM

 	DB_T_PROD_STAG.ab_abcontact

	LEFT OUTER JOIN DB_T_PROD_STAG.abtl_abcontact ON abtl_abcontact.id_stg = ab_abcontact.subtype_stg 

	LEFT OUTER JOIN DB_T_PROD_STAG.ab_user ON ab_user.ContactID_stg = ab_abcontact.id_stg

WHERE

ab_abcontact.UpdateTime_stg>(:start_dttm) AND ab_abcontact.UpdateTime_stg <= (:end_dttm))

 ab_abcontact

 where Retired=0

/* ------------------------------------------------ */
UNION

/* ------------------------------------------------ */
/*  WorkPhone */
SELECT  ab_abcontact.workPhone as phone,

Retired as Retired,createtime as createtime

FROM

(SELECT 

bc_contact.HomePhone_stg HomePhone, /*  01 */
bc_contact.CellPhone_stg CellPhone, /*  02  */
bc_contact.WorkPhone_stg WorkPhone, /* 03 */
bc_contact.PrimaryPhone_stg PrimaryPhone, /* 04 */
bc_contact.Retired_stg Retired, /* 05 */
bc_contact.CreateTime_stg CreateTime/* 06 */
FROM

	DB_T_PROD_STAG.bc_contact 

	LEFT OUTER JOIN DB_T_PROD_STAG.bctl_contact ON bctl_contact.id_stg = bc_contact.subtype_stg 

	LEFT OUTER JOIN DB_T_PROD_STAG.bc_user ON bc_user.ContactID_stg = bc_contact.id_stg

WHERE

	bctl_contact.typecode_stg = (''UserContact'')  AND 

/*  below condition added to avoid duplicates */
    bc_contact.publicid_stg not in (''default_data:1'', ''systemTables:1'',''systemTables:2'') 

	and ((bc_contact.UpdateTime_stg>(:start_dttm) AND bc_contact.UpdateTime_stg <=(:end_dttm)) OR (bc_user.UpdateTime_stg>(:start_dttm) AND bc_user.UpdateTime_stg <= (:end_dttm)))

	

UNION



/*  Primary and Secondary Payer contact (this is at the Account level) */


SELECT 

bc_contact.HomePhone_stg HomePhone, /* 01 */
bc_contact.CellPhone_stg CellPhone, /* 02  */
bc_contact.WorkPhone_stg WorkPhone, /* 03 */
bc_contact.PrimaryPhone_stg PrimaryPhone, /* 04  */
bc_contact.Retired_stg Retired, /* 05  */
bc_contact.CreateTime_stg CreateTime/* 06  */


from DB_T_PROD_STAG.bc_account a

inner join DB_T_PROD_STAG.bc_accountcontact h on h.AccountID_stg = a.id_stg

inner join DB_T_PROD_STAG.bc_contact on bc_contact.id_stg = h.ContactID_stg

join DB_T_PROD_STAG.bctl_contact on bctl_contact.id_stg=bc_contact.subtype_stg

left join DB_T_PROD_STAG.bc_accountcontactrole i on i.AccountContactID_stg = h.id_stg

left join DB_T_PROD_STAG.bctl_accountrole j on j.id_stg = i.Role_stg

LEFT OUTER JOIN DB_T_PROD_STAG.bc_user ON bc_user.ContactID_stg = bc_contact.id_stg

where ((h.PrimaryPayer_stg = 1) or (j.name_stg = ''Payer''))

and ((bc_contact.UpdateTime_stg>(:start_dttm) AND bc_contact.UpdateTime_stg <=(:end_dttm)) OR (bc_user.UpdateTime_stg>(:start_dttm) AND bc_user.UpdateTime_stg <= (:end_dttm)))



UNION

/*  Primary Payer and Overiding Payer Contact (this is at the Invoicestream level) */


SELECT 

bc_contact.HomePhone_stg HomePhone, /* 01  */
bc_contact.CellPhone_stg CellPhone, /* 02  */
bc_contact.WorkPhone_stg WorkPhone, /* 03  */
bc_contact.PrimaryPhone_stg PrimaryPhone, /* 04  */
bc_contact.Retired_stg Retired, /* 05  */
bc_contact.CreateTime_stg CreateTime/* 06 */
from DB_T_PROD_STAG.bc_account a

inner join DB_T_PROD_STAG.bc_invoicestream b on a.id_stg = b.AccountID_stg

inner join DB_T_PROD_STAG.bc_accountcontact c on c.AccountID_stg=a.id_stg

inner join DB_T_PROD_STAG.bc_contact on bc_contact.id_stg = c.ContactID_stg

join DB_T_PROD_STAG.bctl_contact on bctl_contact.id_stg=bc_contact.subtype_stg

LEFT OUTER JOIN DB_T_PROD_STAG.bc_user ON bc_user.ContactID_stg = bc_contact.id_stg

where ((b.OverridingPayer_alfa_stg is null and c.PrimaryPayer_stg = 1) or (b.OverridingPayer_alfa_stg is not null))

and ((bc_contact.UpdateTime_stg>(:start_dttm) AND bc_contact.UpdateTime_stg <=(:end_dttm)) OR (bc_user.UpdateTime_stg>(:start_dttm) AND bc_user.UpdateTime_stg <= (:end_dttm)))



UNION



SELECT 

pc_contact.HomePhone_stg HomePhone, /* 01  */
pc_contact.CellPhone_stg CellPhone, /* 02  */
pc_contact.WorkPhone_stg WorkPhone, /* 03  */
pc_contact.PrimaryPhone_stg PrimaryPhone, /* 04  */
pc_contact.Retired_stg Retired, /* 05  */
pc_contact.CreateTime_stg CreateTime/* 06  */
FROM DB_T_PROD_STAG.pc_contact 

	LEFT OUTER JOIN DB_T_PROD_STAG.pctl_contact ON pctl_contact.id_stg = pc_contact.subtype_stg 

	LEFT OUTER JOIN DB_T_PROD_STAG.pc_user ON pc_user.ContactID_stg = pc_contact.id_stg

WHERE pctl_contact.typecode_stg = (''UserContact'')  AND 

/*  below condition added to avoid duplicates */
       pc_contact.publicid_stg not in (''default_data:1'', ''systemTables:1'',''systemTables:2'') 

	 AND ((pc_contact.UpdateTime_stg>(:start_dttm) AND pc_contact.UpdateTime_stg <= (:end_dttm)) OR (pc_user.UpdateTime_stg>(:start_dttm) AND pc_user.UpdateTime_stg <= (:end_dttm)))

	 

UNION



SELECT DISTINCT 

cc_contact.HomePhone_stg HomePhone, /* 01  */
cc_contact.CellPhone_stg CellPhone, /* 02  */
cc_contact.WorkPhone_stg WorkPhone, /* 03  */
cc_contact.PrimaryPhone_stg PrimaryPhone, /* 04  */
cc_contact.Retired_stg Retired, /* 05  */
cc_contact.CreateTime_stg CreateTime/* 06  */
FROM

	DB_T_PROD_STAG.cc_contact 

	LEFT OUTER JOIN DB_T_PROD_STAG.cctl_contact ON cctl_contact.id_stg = cc_contact.subtype_stg 

	LEFT OUTER JOIN DB_T_PROD_STAG.cc_user ON cc_user.ContactID_stg = cc_contact.id_stg

WHERE

	(cc_contact.UpdateTime_stg>(:start_dttm) AND cc_contact.UpdateTime_stg <= (:end_dttm) )  or 

       (cc_user.UpdateTime_stg>(:start_dttm) AND cc_user.UpdateTime_stg <=(:end_dttm) )

	   

UNION



SELECT 

ab_abcontact.HomePhone_stg HomePhone, /* 01  */
ab_abcontact.CellPhone_stg CellPhone, /* 02  */
ab_abcontact.WorkPhone_stg WorkPhone, /* 03 */
ab_abcontact.PrimaryPhone_stg PrimaryPhone, /* 04 */
ab_abcontact.Retired_stg Retired, /* 05  */
ab_abcontact.CreateTime_stg CreateTime/* 06 */
FROM

 	DB_T_PROD_STAG.ab_abcontact

	LEFT OUTER JOIN DB_T_PROD_STAG.abtl_abcontact ON abtl_abcontact.id_stg = ab_abcontact.subtype_stg 

	LEFT OUTER JOIN DB_T_PROD_STAG.ab_user ON ab_user.ContactID_stg = ab_abcontact.id_stg

WHERE

ab_abcontact.UpdateTime_stg>(:start_dttm) AND ab_abcontact.UpdateTime_stg <= (:end_dttm))

 ab_abcontact

 where Retired=0

/* ---------------------------------------------------------- */
 UNION

/* ----------------------------------------------------------- */
/*  PrimaryPhone  */
 SELECT  cast(ab_abcontact.primaryphone as varchar(30))  as phone,

Retired as Retired,createtime as createtime

FROM

(SELECT 

bc_contact.HomePhone_stg HomePhone, /*  01 */
bc_contact.CellPhone_stg CellPhone, /*  02  */
bc_contact.WorkPhone_stg WorkPhone, /* 03 */
bc_contact.PrimaryPhone_stg PrimaryPhone, /* 04 */
bc_contact.Retired_stg Retired, /* 05 */
bc_contact.CreateTime_stg CreateTime/* 06 */
FROM

	DB_T_PROD_STAG.bc_contact 

	LEFT OUTER JOIN DB_T_PROD_STAG.bctl_contact ON bctl_contact.id_stg = bc_contact.subtype_stg 

	LEFT OUTER JOIN DB_T_PROD_STAG.bc_user ON bc_user.ContactID_stg = bc_contact.id_stg

WHERE

	bctl_contact.typecode_stg = (''UserContact'')  AND 

/*  below condition added to avoid duplicates */
    bc_contact.publicid_stg not in (''default_data:1'', ''systemTables:1'',''systemTables:2'') 

	and ((bc_contact.UpdateTime_stg>(:start_dttm) AND bc_contact.UpdateTime_stg <=(:end_dttm)) OR (bc_user.UpdateTime_stg>(:start_dttm) AND bc_user.UpdateTime_stg <= (:end_dttm)))

	

UNION



/*  Primary and Secondary Payer contact (this is at the Account level) */


SELECT 

bc_contact.HomePhone_stg HomePhone, /* 01 */
bc_contact.CellPhone_stg CellPhone, /* 02  */
bc_contact.WorkPhone_stg WorkPhone, /* 03 */
bc_contact.PrimaryPhone_stg PrimaryPhone, /* 04  */
bc_contact.Retired_stg Retired, /* 05  */
bc_contact.CreateTime_stg CreateTime/* 06  */


from DB_T_PROD_STAG.bc_account a

inner join DB_T_PROD_STAG.bc_accountcontact h on h.AccountID_stg = a.id_stg

inner join DB_T_PROD_STAG.bc_contact on bc_contact.id_stg = h.ContactID_stg

join DB_T_PROD_STAG.bctl_contact on bctl_contact.id_stg=bc_contact.subtype_stg

left join DB_T_PROD_STAG.bc_accountcontactrole i on i.AccountContactID_stg = h.id_stg

left join DB_T_PROD_STAG.bctl_accountrole j on j.id_stg = i.Role_stg

LEFT OUTER JOIN DB_T_PROD_STAG.bc_user ON bc_user.ContactID_stg = bc_contact.id_stg

where ((h.PrimaryPayer_stg = 1) or (j.name_stg = ''Payer''))

and ((bc_contact.UpdateTime_stg>(:start_dttm) AND bc_contact.UpdateTime_stg <=(:end_dttm)) OR (bc_user.UpdateTime_stg>(:start_dttm) AND bc_user.UpdateTime_stg <= (:end_dttm)))



UNION

/*  Primary Payer and Overiding Payer Contact (this is at the Invoicestream level) */


SELECT 

bc_contact.HomePhone_stg HomePhone, /* 01  */
bc_contact.CellPhone_stg CellPhone, /* 02  */
bc_contact.WorkPhone_stg WorkPhone, /* 03  */
bc_contact.PrimaryPhone_stg PrimaryPhone, /* 04  */
bc_contact.Retired_stg Retired, /* 05  */
bc_contact.CreateTime_stg CreateTime/* 06 */
from DB_T_PROD_STAG.bc_account a

inner join DB_T_PROD_STAG.bc_invoicestream b on a.id_stg = b.AccountID_stg

inner join DB_T_PROD_STAG.bc_accountcontact c on c.AccountID_stg=a.id_stg

inner join DB_T_PROD_STAG.bc_contact on bc_contact.id_stg = c.ContactID_stg

join DB_T_PROD_STAG.bctl_contact on bctl_contact.id_stg=bc_contact.subtype_stg

LEFT OUTER JOIN DB_T_PROD_STAG.bc_user ON bc_user.ContactID_stg = bc_contact.id_stg

where ((b.OverridingPayer_alfa_stg is null and c.PrimaryPayer_stg = 1) or (b.OverridingPayer_alfa_stg is not null))

and ((bc_contact.UpdateTime_stg>(:start_dttm) AND bc_contact.UpdateTime_stg <=(:end_dttm)) OR (bc_user.UpdateTime_stg>(:start_dttm) AND bc_user.UpdateTime_stg <= (:end_dttm)))



UNION



SELECT 

pc_contact.HomePhone_stg HomePhone, /* 01  */
pc_contact.CellPhone_stg CellPhone, /* 02  */
pc_contact.WorkPhone_stg WorkPhone, /* 03  */
pc_contact.PrimaryPhone_stg PrimaryPhone, /* 04  */
pc_contact.Retired_stg Retired, /* 05  */
pc_contact.CreateTime_stg CreateTime/* 06  */
FROM DB_T_PROD_STAG.pc_contact 

	LEFT OUTER JOIN DB_T_PROD_STAG.pctl_contact ON pctl_contact.id_stg = pc_contact.subtype_stg 

	LEFT OUTER JOIN DB_T_PROD_STAG.pc_user ON pc_user.ContactID_stg = pc_contact.id_stg

WHERE pctl_contact.typecode_stg = (''UserContact'')  AND 

/*  below condition added to avoid duplicates */
       pc_contact.publicid_stg not in (''default_data:1'', ''systemTables:1'',''systemTables:2'') 

	 AND ((pc_contact.UpdateTime_stg>(:start_dttm) AND pc_contact.UpdateTime_stg <= (:end_dttm)) OR (pc_user.UpdateTime_stg>(:start_dttm) AND pc_user.UpdateTime_stg <= (:end_dttm)))

	 

UNION



SELECT DISTINCT 

cc_contact.HomePhone_stg HomePhone, /* 01  */
cc_contact.CellPhone_stg CellPhone, /* 02  */
cc_contact.WorkPhone_stg WorkPhone, /* 03  */
cc_contact.PrimaryPhone_stg PrimaryPhone, /* 04  */
cc_contact.Retired_stg Retired, /* 05  */
cc_contact.CreateTime_stg CreateTime/* 06  */
FROM

	DB_T_PROD_STAG.cc_contact 

	LEFT OUTER JOIN DB_T_PROD_STAG.cctl_contact ON cctl_contact.id_stg = cc_contact.subtype_stg 

	LEFT OUTER JOIN DB_T_PROD_STAG.cc_user ON cc_user.ContactID_stg = cc_contact.id_stg

WHERE

	(cc_contact.UpdateTime_stg>(:start_dttm) AND cc_contact.UpdateTime_stg <= (:end_dttm) )  or 

       (cc_user.UpdateTime_stg>(:start_dttm) AND cc_user.UpdateTime_stg <=(:end_dttm) )

	   

UNION



SELECT 

ab_abcontact.HomePhone_stg HomePhone, /* 01  */
ab_abcontact.CellPhone_stg CellPhone, /* 02  */
ab_abcontact.WorkPhone_stg WorkPhone, /* 03 */
ab_abcontact.PrimaryPhone_stg PrimaryPhone, /* 04 */
ab_abcontact.Retired_stg Retired, /* 05  */
ab_abcontact.CreateTime_stg CreateTime/* 06 */
FROM

 	DB_T_PROD_STAG.ab_abcontact

	LEFT OUTER JOIN DB_T_PROD_STAG.abtl_abcontact ON abtl_abcontact.id_stg = ab_abcontact.subtype_stg 

	LEFT OUTER JOIN DB_T_PROD_STAG.ab_user ON ab_user.ContactID_stg = ab_abcontact.id_stg

WHERE

ab_abcontact.UpdateTime_stg>(:start_dttm) AND ab_abcontact.UpdateTime_stg <= (:end_dttm))

 ab_abcontact

 where Retired=0



) A  where A.phone is not null QUALIFY ROW_NUMBER( ) OVER ( PARTITION BY PHONE ORDER BY createtime  DESC ) =1
) SRC
)
);


-- Component exp_pass_from_source, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_from_source AS
(
SELECT
sq_pc_contact.Phone as HomePhone,
LKP_1.TLPHN_NUM_ID /* replaced lookup LKP_TLPHN_NUM */ as out_HomePhone,
sq_pc_contact.Retired as Retired,
sq_pc_contact.source_record_id,
row_number() over (partition by sq_pc_contact.source_record_id order by sq_pc_contact.source_record_id) as RNK
FROM
sq_pc_contact
LEFT JOIN LKP_TLPHN_NUM LKP_1 ON LKP_1.TLPHN_NUM = sq_pc_contact.Phone
QUALIFY RNK = 1
);


-- Component exp_tlphn_num, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_tlphn_num AS
(
SELECT
exp_pass_from_source.HomePhone as HomePhone,
SEQ_loc.NEXTVAL as var_TLPHN_NUM_ID,
''LOCTR_SBTYPE1'' as var_loctr_sbtype_val,
''ADDR_SBTYPE3'' as var_address_sbtype_val,
SUBSTR ( exp_pass_from_source.HomePhone , 0 , 3 ) as out_tlphn_area_cd_num,
var_TLPHN_NUM_ID as out_TLPHN_NUM_ID,
LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_LOCTR_SBTYPE */ as out_loctr_sbtype,
LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ADDRESS_SBTYPE */ as out_address_sbtype,
:PRCS_ID as o_process_id,
CURRENT_TIMESTAMP as EDW_STRT_DTTM,
to_timestamp ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as EDW_END_DTTM,
exp_pass_from_source.Retired as Retired,
exp_pass_from_source.source_record_id,
row_number() over (partition by exp_pass_from_source.source_record_id order by exp_pass_from_source.source_record_id) as RNK
FROM
exp_pass_from_source
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_LOCTR_SBTYPE LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = var_loctr_sbtype_val
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ADDRESS_SBTYPE LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = var_address_sbtype_val
QUALIFY RNK = 1
);


-- Component LKP_TLPHN_NUM_TGT, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_TLPHN_NUM_TGT AS
(
SELECT
LKP.TLPHN_AREA_CD_NUM,
LKP.LOCTR_SBTYPE_CD,
LKP.ADDR_SBTYPE_CD,
exp_tlphn_num.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_tlphn_num.source_record_id ORDER BY LKP.TLPHN_AREA_CD_NUM asc,LKP.LOCTR_SBTYPE_CD asc,LKP.ADDR_SBTYPE_CD asc) RNK
FROM
exp_tlphn_num
LEFT JOIN (
SELECT  
        TLPHN_NUM.TLPHN_NUM as TLPHN_NUM ,
                TLPHN_NUM.TLPHN_AREA_CD_NUM as TLPHN_AREA_CD_NUM, 
                        TLPHN_NUM.LOCTR_SBTYPE_CD as LOCTR_SBTYPE_CD,       
                            TLPHN_NUM.ADDR_SBTYPE_CD as ADDR_SBTYPE_CD
                            FROM    DB_T_PROD_CORE.TLPHN_NUM 
                                WHERE CAST(TLPHN_NUM.EDW_END_DTTM AS DATE)=CAST(''9999-12-31'' AS DATE)
) LKP ON LKP.TLPHN_NUM = exp_tlphn_num.HomePhone
QUALIFY RNK = 1
);


-- Component exp_compare_data, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_compare_data AS
(
SELECT
MD5 ( LTRIM ( RTRIM ( LKP_TLPHN_NUM_TGT.TLPHN_AREA_CD_NUM ) ) || LTRIM ( RTRIM ( LKP_TLPHN_NUM_TGT.LOCTR_SBTYPE_CD ) ) || LTRIM ( RTRIM ( LKP_TLPHN_NUM_TGT.ADDR_SBTYPE_CD ) ) ) as v_lkp_checksum,
exp_tlphn_num.out_TLPHN_NUM_ID as in_TLPHN_NUM_ID,
exp_tlphn_num.HomePhone as in_HomePhone,
exp_tlphn_num.out_tlphn_area_cd_num as in_tlphn_area_cd_num,
exp_tlphn_num.out_loctr_sbtype as in_loctr_sbtype,
exp_tlphn_num.out_address_sbtype as in_address_sbtype,
exp_tlphn_num.o_process_id as in_process_id,
exp_tlphn_num.EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_tlphn_num.EDW_END_DTTM as in_EDW_END_DTTM,
MD5 ( LTRIM ( RTRIM ( exp_tlphn_num.out_tlphn_area_cd_num ) ) || LTRIM ( RTRIM ( exp_tlphn_num.out_loctr_sbtype ) ) || LTRIM ( RTRIM ( exp_tlphn_num.out_address_sbtype ) ) ) as v_in_checksum,
CASE WHEN v_lkp_checksum IS NULL THEN ''I'' ELSE CASE WHEN v_lkp_checksum != v_in_checksum THEN ''U'' ELSE ''R'' END END as calc_ins_upd,
exp_tlphn_num.Retired as Retired,
exp_tlphn_num.source_record_id
FROM
exp_tlphn_num
INNER JOIN LKP_TLPHN_NUM_TGT ON exp_tlphn_num.source_record_id = LKP_TLPHN_NUM_TGT.source_record_id
);


-- Component rtr_tlphn_num_INSERT, Type ROUTER Output Group INSERT
CREATE OR REPLACE TEMPORARY TABLE rtr_tlphn_num_INSERT AS
(SELECT
exp_compare_data.in_TLPHN_NUM_ID as in_TLPHN_NUM_ID,
exp_compare_data.in_HomePhone as in_HomePhone,
exp_compare_data.in_tlphn_area_cd_num as in_tlphn_area_cd_num,
exp_compare_data.in_loctr_sbtype as in_loctr_sbtype,
exp_compare_data.in_address_sbtype as in_address_sbtype,
exp_compare_data.in_process_id as in_process_id,
exp_compare_data.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_compare_data.in_EDW_END_DTTM as in_EDW_END_DTTM,
exp_compare_data.calc_ins_upd as calc_ins_upd,
exp_compare_data.Retired as Retired,
exp_compare_data.source_record_id
FROM
exp_compare_data
WHERE ( exp_compare_data.calc_ins_upd = ''I'' ) 
-- or ( exp_compare_data.calc_ins_upd = ''U'' ) 
-- AND lkp_EDW_END_DTTM = TO_TIMESTAMP ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) 
);


-- Component upd_tlphn_num_insert, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_tlphn_num_insert AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_tlphn_num_INSERT.in_TLPHN_NUM_ID as in_TLPHN_NUM_ID1,
rtr_tlphn_num_INSERT.in_HomePhone as in_HomePhone1,
rtr_tlphn_num_INSERT.in_tlphn_area_cd_num as in_tlphn_area_cd_num1,
rtr_tlphn_num_INSERT.in_loctr_sbtype as in_loctr_sbtype1,
rtr_tlphn_num_INSERT.in_address_sbtype as in_address_sbtype1,
rtr_tlphn_num_INSERT.in_process_id as in_process_id1,
rtr_tlphn_num_INSERT.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM1,
rtr_tlphn_num_INSERT.in_EDW_END_DTTM as in_EDW_END_DTTM1,
rtr_tlphn_num_INSERT.Retired as Retired1,
0 as UPDATE_STRATEGY_ACTION,
rtr_tlphn_num_INSERT.source_record_id
FROM
rtr_tlphn_num_INSERT
);


-- Component exp_tlphn_num_insert, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_tlphn_num_insert AS
(
SELECT
upd_tlphn_num_insert.in_TLPHN_NUM_ID1 as in_TLPHN_NUM_ID1,
upd_tlphn_num_insert.in_HomePhone1 as in_HomePhone1,
upd_tlphn_num_insert.in_tlphn_area_cd_num1 as in_tlphn_area_cd_num1,
upd_tlphn_num_insert.in_loctr_sbtype1 as in_loctr_sbtype1,
upd_tlphn_num_insert.in_address_sbtype1 as in_address_sbtype1,
upd_tlphn_num_insert.in_process_id1 as in_process_id1,
upd_tlphn_num_insert.in_EDW_STRT_DTTM1 as in_EDW_STRT_DTTM1,
upd_tlphn_num_insert.in_EDW_END_DTTM1 as in_EDW_END_DTTM1,
upd_tlphn_num_insert.source_record_id
FROM
upd_tlphn_num_insert
);


-- Component tgt_tlphn_num_insert, Type TARGET 
INSERT INTO DB_T_PROD_CORE.TLPHN_NUM
(
TLPHN_NUM_ID,
TLPHN_NUM,
TLPHN_AREA_CD_NUM,
PRCS_ID,
LOCTR_SBTYPE_CD,
ADDR_SBTYPE_CD,
EDW_STRT_DTTM,
EDW_END_DTTM
)
SELECT
exp_tlphn_num_insert.in_TLPHN_NUM_ID1 as TLPHN_NUM_ID,
exp_tlphn_num_insert.in_HomePhone1 as TLPHN_NUM,
exp_tlphn_num_insert.in_tlphn_area_cd_num1 as TLPHN_AREA_CD_NUM,
exp_tlphn_num_insert.in_process_id1 as PRCS_ID,
exp_tlphn_num_insert.in_loctr_sbtype1 as LOCTR_SBTYPE_CD,
exp_tlphn_num_insert.in_address_sbtype1 as ADDR_SBTYPE_CD,
exp_tlphn_num_insert.in_EDW_STRT_DTTM1 as EDW_STRT_DTTM,
exp_tlphn_num_insert.in_EDW_END_DTTM1 as EDW_END_DTTM
FROM
exp_tlphn_num_insert;


END; ';