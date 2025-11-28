-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_ALFA_LIFT_035_OFAC_DEDUP("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' BEGIN 

-- Component TD_OFAC_Stag_DeDup, Type TRUNCATE_TABLE 
--TRUNCATE TABLE TD_OFAC_Stag_DeDup;


-- Component SQ_OFAC_Stag, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_OFAC_Stag AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as FirstName,
$2 as LastName,
$3 as FullName,
$4 as AddressLine1,
$5 as AddressLine2,
$6 as AddressLine3,
$7 as City,
$8 as StateCD,
$9 as PostalCode,
$10 as ContactType,
$11 as TaxID,
$12 as ClaimNumber,
$13 as PolicyNumber,
$14 as AccountNumber,
$15 as SourceSystem,
$16 as UpdateTime,
$17 as SourceGroup,
$18 as TaxStatus,
$19 as Country,
$20 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
WITH IRS_DATA_TEMP AS (SELECT  * FROM
(SELECT  final_dedup.FirstName AS FirstName,final_dedup.LastName AS LastName, final_dedup.FullName AS FullName,final_dedup.AddressLine1 AS AddressLine1,
 final_dedup.AddressLine2 AS AddressLine2,final_dedup.AddressLine3 AS AddressLine3,City AS City,final_dedup.StateCD AS StateCD,
 final_dedup.PostalCode AS PostalCode, final_dedup.ContactType AS ContactType,final_dedup.TaxID AS TaxID, final_dedup.ClaimNumber AS ClaimNumber,
final_dedup.PolicyNumber AS PolicyNumber,final_dedup.AccountNumber AS AccountNumber,final_dedup.SourceSystem AS SourceSystem,
 final_dedup.AddressUpdateTime AS UpdateTime, final_dedup.SourceGroup AS SourceGroup, final_dedup.TaxStatus AS TaxStatus, final_dedup.Country AS Country
FROM 
/* Get only the matched records for people */
 (SELECT deduped.FirstName AS FirstName, deduped.LastName AS LastName, deduped.FullName AS FullName, deduped.AddressLine1 AS AddressLine1,
 deduped.AddressLine2 AS AddressLine2,deduped.AddressLine3 AS AddressLine3, City AS City, deduped.StateCD AS StateCD, deduped.PostalCode AS PostalCode,
 deduped.ContactType AS ContactType, deduped.ClaimNumber AS ClaimNumber, deduped.PolicyNumber AS PolicyNumber, deduped.AccountNumber AS AccountNumber,
 deduped.TaxID AS TaxID, deduped.SourceSystem AS SourceSystem, deduped.UpdateTime AS AddressUpdateTime,deduped.SourceGroup AS SourceGroup,
 deduped.TaxStatus AS TaxStatus, deduped.Country AS Country FROM 
/* Get rank by updatetime partitioned by match INFORMATION_SCHEMA.fields to get the newest record */
 ( SELECT most_recent_rank.*, Row_Number()  Over( PARTITION BY most_recent_rank.FirstName, most_recent_rank.LastName,
 most_recent_rank.FullName, most_recent_rank.AddressLine1, most_recent_rank.AddressLine2,most_recent_rank.AddressLine3, City,
 most_recent_rank.StateCD , most_recent_rank.TaxID,most_recent_rank.Country  ORDER BY most_recent_rank.UpdateTime DESC) AS rnk_nbr 
  FROM DB_T_PROD_STAG.OFAC_Stag AS most_recent_rank 
/*     where sourcesystem in (''BillingCenter'',''ClaimsCenter'',''PolicyCenter'')  */
WHERE sourcesystem IN (''BillingCenter'',''PolicyCenter'') 
  OR (sourcesystem = ''ClaimsCenter'' AND NOT((ContactType = ''Company''  AND TaxStatus = ''1'')  
 OR (ContactType = ''CompanyVendor''  AND TaxStatus = ''1'')))  ) AS deduped 
 WHERE deduped.rnk_nbr = 1 ) final_dedup
UNION
SELECT final_dedup.FirstName AS FirstName,final_dedup.LastName AS LastName,final_dedup.FullName AS FullName, final_dedup.AddressLine1 AS AddressLine1, 
 final_dedup.AddressLine2 AS AddressLine2,final_dedup.AddressLine3 AS AddressLine3,City AS City,final_dedup.StateCD AS StateCD, 
 final_dedup.PostalCode AS PostalCode, final_dedup.ContactType AS ContactType,final_dedup.TaxID AS TaxID, final_dedup.ClaimNumber AS ClaimNumber, 
 final_dedup.PolicyNumber AS PolicyNumber,final_dedup.AccountNumber AS AccountNumber,final_dedup.SourceSystem AS SourceSystem, final_dedup.AddressUpdateTime AS UpdateTime,
  final_dedup.SourceGroup AS SourceGroup,  final_dedup.TaxStatus AS TaxStatus,  final_dedup.Country AS Country
FROM (
 SELECT deduped.FirstName AS FirstName, deduped.LastName AS LastName, deduped.FullName AS FullName, deduped.AddressLine1 AS AddressLine1,
 deduped.AddressLine2 AS AddressLine2,deduped.AddressLine3 AS AddressLine3, City AS City, deduped.StateCD AS StateCD, deduped.PostalCode AS PostalCode,
 deduped.ContactType AS ContactType, deduped.ClaimNumber AS ClaimNumber, deduped.PolicyNumber AS PolicyNumber, deduped.AccountNumber AS AccountNumber,
 deduped.TaxID AS TaxID, deduped.SourceSystem AS SourceSystem, deduped.UpdateTime AS AddressUpdateTime,deduped.SourceGroup AS SourceGroup,
 deduped.TaxStatus AS TaxStatus, deduped.Country AS Country
 FROM 
/* Get rank by updatetime partitioned by match INFORMATION_SCHEMA.fields to get the newest record */
 ( SELECT most_recent_rank.*, Row_Number()  Over( PARTITION BY most_recent_rank.FirstName, most_recent_rank.LastName, most_recent_rank.FullName, most_recent_rank.TaxID, most_recent_rank.Country
 ORDER BY most_recent_rank.UpdateTime DESC) AS rnk_nbr 
  FROM DB_T_PROD_STAG.OFAC_Stag AS most_recent_rank 
   WHERE sourcesystem = ''ClaimsCenter''  AND ((ContactType = ''Company''  AND TaxStatus = ''1'')  
 OR (ContactType = ''CompanyVendor''  AND TaxStatus = ''1'')) ) AS deduped
 WHERE deduped.rnk_nbr = 1 ) final_dedup
UNION 
SELECT  FirstName,LastName,FullName,AddressLine1,AddressLine2,AddressLine3,City,StateCD,PostalCode, ContactType, 
 TaxID, ClaimNumber,PolicyNumber,AccountNumber,SourceSystem, UpdateTime, SourceGroup, TaxStatus, Country
  FROM DB_T_PROD_STAG.OFAC_Stag   WHERE sourcesystem NOT IN (''BillingCenter'', ''ClaimsCenter'',''PolicyCenter'')   )a)

/*  Main Query */
SELECT * FROM(
SELECT  DISTINCT FIRSTNAME, LASTNAME, FULLNAME,ADDRESSLINE1, ADDRESSLINE2, ADDRESSLINE3, City, STATECD, POSTALCODE, CONTACTTYPE, 
TAXID, CLAIMNUMBER, POLICYNUMBER, ACCOUNTNUMBER, SOURCESYSTEM, UPDATETIME, SOURCEGROUP, TAXSTATUS, COUNTRY 
FROM IRS_DATA_TEMP WHERE SOURCESYSTEM NOT IN (''IRS'')
UNION 
SELECT  DISTINCT FIRSTNAME, LASTNAME, FULLNAME,ADDRESSLINE1, ADDRESSLINE2, ADDRESSLINE3, City, STATECD, POSTALCODE, CONTACTTYPE, 
TAXID, CLAIMNUMBER, POLICYNUMBER, ACCOUNTNUMBER, SOURCESYSTEM, UPDATETIME, SOURCEGROUP, TAXSTATUS, COUNTRY 
FROM 
(SELECT DISTINCT A.* FROM  IRS_DATA_TEMP AS A WHERE A.SOURCESYSTEM =''IRS'') B
 LEFT JOIN (SELECT CASE WHEN FULLNAME IS NOT NULL THEN FULLNAME ELSE (LASTNAME||'',''||'' ''||FIRSTNAME) END AS FULL_NAME,
ADDRESSLINE1 ADDRESSLINE_1,City CITY_NEW, STATECD STATE_CD, REPLACE(TAXID,''-'','''') AS TAX_ID,
SUBSTR(POSTALCODE,1,5) AS POSTAL_CODE
FROM IRS_DATA_TEMP WHERE SOURCESYSTEM =''CLAIMSCENTER'') A
ON  COALESCE(TRIM(A.FULL_NAME),'''')=COALESCE(TRIM(B.FULLNAME),'''')
AND COALESCE(TRIM(A.ADDRESSLINE_1),'''')=COALESCE(TRIM(B.ADDRESSLINE1),'''')
AND COALESCE(TRIM(A.CITY_NEW),'''')=COALESCE(TRIM(City),'''')
AND COALESCE(TRIM(A.STATE_CD),'''')=COALESCE(TRIM(B.STATECD),'''')
AND COALESCE(TRIM(A.TAX_ID),'''')=COALESCE(TRIM(B.TAXID) ,'''')
AND COALESCE(TRIM(A.POSTAL_CODE),'''')=COALESCE(TRIM(B.POSTALCODE) ,'''')
WHERE A.FULL_NAME IS NULL) AS A
) SRC
)
);


-- Component EXPTRANS, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXPTRANS AS
(
SELECT
SQ_OFAC_Stag.FirstName as FirstName,
SQ_OFAC_Stag.LastName as LastName,
SQ_OFAC_Stag.FullName as FullName,
SQ_OFAC_Stag.AddressLine1 as AddressLine1,
SQ_OFAC_Stag.AddressLine2 as AddressLine2,
SQ_OFAC_Stag.AddressLine3 as AddressLine3,
SQ_OFAC_Stag.City as City,
SQ_OFAC_Stag.StateCD as StateCD,
SQ_OFAC_Stag.PostalCode as PostalCode,
SQ_OFAC_Stag.ContactType as ContactType,
SQ_OFAC_Stag.TaxID as TaxID,
SQ_OFAC_Stag.ClaimNumber as ClaimNumber,
SQ_OFAC_Stag.PolicyNumber as PolicyNumber,
SQ_OFAC_Stag.AccountNumber as AccountNumber,
SQ_OFAC_Stag.SourceSystem as SourceSystem,
SQ_OFAC_Stag.UpdateTime as UpdateTime,
SQ_OFAC_Stag.SourceGroup as SourceGroup,
SQ_OFAC_Stag.TaxStatus as TaxStatus,
ltrim ( rtrim ( SQ_OFAC_Stag.Country ) ) as o_Country,
SQ_OFAC_Stag.source_record_id
FROM
SQ_OFAC_Stag
);


-- Component RTRTRANS_ALL_OTHERS, Type ROUTER Output Group ALL_OTHERS
create or replace temporary table RTRTRANS_ALL_OTHERS AS
SELECT
EXPTRANS.FirstName as FirstName,
EXPTRANS.LastName as LastName,
EXPTRANS.FullName as FullName,
EXPTRANS.AddressLine1 as AddressLine1,
EXPTRANS.AddressLine2 as AddressLine2,
EXPTRANS.AddressLine3 as AddressLine3,
EXPTRANS.City as City,
EXPTRANS.StateCD as StateCD,
EXPTRANS.PostalCode as PostalCode,
EXPTRANS.ContactType as ContactType,
EXPTRANS.TaxID as TaxID,
EXPTRANS.ClaimNumber as ClaimNumber,
EXPTRANS.PolicyNumber as PolicyNumber,
EXPTRANS.AccountNumber as AccountNumber,
EXPTRANS.SourceSystem as SourceSystem,
EXPTRANS.UpdateTime as UpdateTime,
EXPTRANS.SourceGroup as SourceGroup,
EXPTRANS.TaxStatus as TaxStatus,
EXPTRANS.o_Country as Country,
EXPTRANS.source_record_id
FROM
EXPTRANS
WHERE EXPTRANS.SourceSystem <> ''ING_POL_LIST'';


-- Component RTRTRANS_ING_POL_LIST, Type ROUTER Output Group ING_POL_LIST
create or replace temporary table RTRTRANS_ING_POL_LIST AS
SELECT
EXPTRANS.FirstName as FirstName,
EXPTRANS.LastName as LastName,
EXPTRANS.FullName as FullName,
EXPTRANS.AddressLine1 as AddressLine1,
EXPTRANS.AddressLine2 as AddressLine2,
EXPTRANS.AddressLine3 as AddressLine3,
EXPTRANS.City as City,
EXPTRANS.StateCD as StateCD,
EXPTRANS.PostalCode as PostalCode,
EXPTRANS.ContactType as ContactType,
EXPTRANS.TaxID as TaxID,
EXPTRANS.ClaimNumber as ClaimNumber,
EXPTRANS.PolicyNumber as PolicyNumber,
EXPTRANS.AccountNumber as AccountNumber,
EXPTRANS.SourceSystem as SourceSystem,
EXPTRANS.UpdateTime as UpdateTime,
EXPTRANS.SourceGroup as SourceGroup,
EXPTRANS.TaxStatus as TaxStatus,
EXPTRANS.o_Country as Country,
EXPTRANS.source_record_id
FROM
EXPTRANS
WHERE EXPTRANS.SourceSystem = ''ING_POL_LIST'';


-- Component AGGTRANS, Type AGGREGATOR 
CREATE OR REPLACE TEMPORARY TABLE AGGTRANS AS
(
SELECT
RTRTRANS_ING_POL_LIST.FirstName as FirstName1,
RTRTRANS_ING_POL_LIST.LastName as LastName1,
RTRTRANS_ING_POL_LIST.FullName as FullName1,
RTRTRANS_ING_POL_LIST.AddressLine1 as AddressLine11,
RTRTRANS_ING_POL_LIST.AddressLine2 as AddressLine21,
RTRTRANS_ING_POL_LIST.AddressLine3 as AddressLine31,
RTRTRANS_ING_POL_LIST.City as City1,
RTRTRANS_ING_POL_LIST.StateCD as StateCD1,
RTRTRANS_ING_POL_LIST.PostalCode as PostalCode1,
RTRTRANS_ING_POL_LIST.ContactType as ContactType1,
RTRTRANS_ING_POL_LIST.TaxID as TaxID1,
RTRTRANS_ING_POL_LIST.ClaimNumber as ClaimNumber1,
MIN(RTRTRANS_ING_POL_LIST.PolicyNumber) as PolicyNumber1,
RTRTRANS_ING_POL_LIST.AccountNumber as AccountNumber1,
RTRTRANS_ING_POL_LIST.SourceSystem as SourceSystem1,
RTRTRANS_ING_POL_LIST.UpdateTime as UpdateTime1,
MIN(RTRTRANS_ING_POL_LIST.SourceGroup) as SourceGroup1,
MIN(RTRTRANS_ING_POL_LIST.TaxStatus) as TaxStatus,
MIN(RTRTRANS_ING_POL_LIST.Country) as Country,
MIN(RTRTRANS_ING_POL_LIST.source_record_id) as source_record_id
FROM
RTRTRANS_ING_POL_LIST
GROUP BY
RTRTRANS_ING_POL_LIST.FirstName,
RTRTRANS_ING_POL_LIST.LastName,
RTRTRANS_ING_POL_LIST.FullName,
RTRTRANS_ING_POL_LIST.AddressLine1,
RTRTRANS_ING_POL_LIST.AddressLine2,
RTRTRANS_ING_POL_LIST.AddressLine3,
RTRTRANS_ING_POL_LIST.City,
RTRTRANS_ING_POL_LIST.StateCD,
RTRTRANS_ING_POL_LIST.PostalCode,
RTRTRANS_ING_POL_LIST.ContactType,
RTRTRANS_ING_POL_LIST.TaxID,
RTRTRANS_ING_POL_LIST.ClaimNumber,
RTRTRANS_ING_POL_LIST.AccountNumber,
RTRTRANS_ING_POL_LIST.SourceSystem,
RTRTRANS_ING_POL_LIST.UpdateTime
);


-- Component Union, Type UNION_TRANSFORMATION 
CREATE OR REPLACE TEMPORARY TABLE "Union" AS
(
/* Union Group NEWGROUP */
SELECT
AGGTRANS.FirstName1,
AGGTRANS.LastName1,
AGGTRANS.FullName1,
AGGTRANS.AddressLine11,
AGGTRANS.AddressLine21,
AGGTRANS.AddressLine31,
AGGTRANS.City1,
AGGTRANS.StateCD1,
AGGTRANS.PostalCode1,
AGGTRANS.ContactType1,
AGGTRANS.TaxID1,
AGGTRANS.ClaimNumber1,
AGGTRANS.PolicyNumber1,
AGGTRANS.AccountNumber1,
AGGTRANS.SourceSystem1,
AGGTRANS.UpdateTime1,
AGGTRANS.SourceGroup1,
AGGTRANS.TaxStatus,
AGGTRANS.Country,
RTRTRANS_ING_POL_LIST.source_record_id
FROM RTRTRANS_ING_POL_LIST
INNER JOIN AGGTRANS ON RTRTRANS_ING_POL_LIST.source_record_id = AGGTRANS.source_record_id
UNION ALL
/* Union Group NEWGROUP1 */
SELECT
RTRTRANS_ALL_OTHERS.FirstName as FirstName1,
RTRTRANS_ALL_OTHERS.LastName as LastName1,
RTRTRANS_ALL_OTHERS.FullName as FullName1,
RTRTRANS_ALL_OTHERS.AddressLine1 as AddressLine11,
RTRTRANS_ALL_OTHERS.AddressLine2 as AddressLine21,
RTRTRANS_ALL_OTHERS.AddressLine3 as AddressLine31,
RTRTRANS_ALL_OTHERS.City as City1,
RTRTRANS_ALL_OTHERS.StateCD as StateCD1,
RTRTRANS_ALL_OTHERS.PostalCode as PostalCode1,
RTRTRANS_ALL_OTHERS.ContactType as ContactType1,
RTRTRANS_ALL_OTHERS.TaxID as TaxID1,
RTRTRANS_ALL_OTHERS.ClaimNumber as ClaimNumber1,
RTRTRANS_ALL_OTHERS.PolicyNumber as PolicyNumber1,
RTRTRANS_ALL_OTHERS.AccountNumber as AccountNumber1,
RTRTRANS_ALL_OTHERS.SourceSystem as SourceSystem1,
RTRTRANS_ALL_OTHERS.UpdateTime as UpdateTime1,
RTRTRANS_ALL_OTHERS.SourceGroup as SourceGroup1,
RTRTRANS_ALL_OTHERS.TaxStatus as TaxStatus,
RTRTRANS_ALL_OTHERS.Country as Country,
RTRTRANS_ALL_OTHERS.source_record_id
FROM RTRTRANS_ALL_OTHERS
);


-- Component SRTTRANS, Type SORTER 
CREATE OR REPLACE TEMPORARY TABLE SRTTRANS AS
(
SELECT
"Union".FirstName1 as FirstName1,
"Union".LastName1 as LastName1,
"Union".FullName1 as FullName1,
"Union".AddressLine11 as AddressLine11,
"Union".AddressLine21 as AddressLine21,
"Union".AddressLine31 as AddressLine31,
"Union".City1 as City1,
"Union".StateCD1 as StateCD1,
"Union".PostalCode1 as PostalCode1,
"Union".ContactType1 as ContactType1,
"Union".TaxID1 as TaxID1,
"Union".ClaimNumber1 as ClaimNumber1,
"Union".PolicyNumber1 as PolicyNumber1,
"Union".AccountNumber1 as AccountNumber1,
"Union".SourceSystem1 as SourceSystem1,
"Union".UpdateTime1 as UpdateTime1,
"Union".SourceGroup1 as SourceGroup1,
"Union".TaxStatus as TaxStatus,
"Union".Country as Country,
"Union".source_record_id
FROM
"Union"
ORDER BY FullName1 
);


-- Component TD_OFAC_Stag_DeDup, Type TARGET 
INSERT INTO DB_T_PROD_STAG.ofac_stag_dedup
(
FirstName,
LastName,
FullName,
AddressLine1,
AddressLine2,
AddressLine3,
City,
StateCD,
PostalCode,
ContactType,
TaxID,
ClaimNumber,
PolicyNumber,
AccountNumber,
SourceSystem,
UpdateTime,
SourceGroup,
TaxStatus,
Country
)
SELECT
SRTTRANS.FirstName1 as FirstName,
SRTTRANS.LastName1 as LastName,
SRTTRANS.FullName1 as FullName,
SRTTRANS.AddressLine11 as AddressLine1,
SRTTRANS.AddressLine21 as AddressLine2,
SRTTRANS.AddressLine31 as AddressLine3,
SRTTRANS.City1 as City,
SRTTRANS.StateCD1 as StateCD,
SRTTRANS.PostalCode1 as PostalCode,
SRTTRANS.ContactType1 as ContactType,
SRTTRANS.TaxID1 as TaxID,
SRTTRANS.ClaimNumber1 as ClaimNumber,
SRTTRANS.PolicyNumber1 as PolicyNumber,
SRTTRANS.AccountNumber1 as AccountNumber,
SRTTRANS.SourceSystem1 as SourceSystem,
SRTTRANS.UpdateTime1 as UpdateTime,
SRTTRANS.SourceGroup1 as SourceGroup,
SRTTRANS.TaxStatus as TaxStatus,
SRTTRANS.Country as Country
FROM
SRTTRANS;


END; ';