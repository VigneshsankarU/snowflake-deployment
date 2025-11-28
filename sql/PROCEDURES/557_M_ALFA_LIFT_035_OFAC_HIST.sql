-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_ALFA_LIFT_035_OFAC_HIST("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS ' DECLARE
  run_id STRING;
  workflow_name STRING;
  session_name STRING;
BEGIN
  run_id := public.func_get_scoped_param(:run_id, ''run_id'', :workflow_name, :worklet_name, :session_name);
  workflow_name := public.func_get_scoped_param(:run_id, ''workflow_name'', :workflow_name, :worklet_name, :session_name);
  session_name := public.func_get_scoped_param(:run_id, ''session_name'', :workflow_name, :worklet_name, :session_name);
 

-- PIPELINE START FOR 1

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
SELECT
OFAC_Stag.FirstName,
OFAC_Stag.LastName,
OFAC_Stag.FullName,
OFAC_Stag.AddressLine1,
OFAC_Stag.AddressLine2,
OFAC_Stag.AddressLine3,
OFAC_Stag.City,
OFAC_Stag.StateCD,
OFAC_Stag.PostalCode,
OFAC_Stag.ContactType,
OFAC_Stag.TaxID,
OFAC_Stag.ClaimNumber,
OFAC_Stag.PolicyNumber,
OFAC_Stag.AccountNumber,
OFAC_Stag.SourceSystem,
OFAC_Stag.UpdateTime,
OFAC_Stag.SourceGroup,
OFAC_Stag.TaxStatus,
OFAC_Stag.Country
FROM OFAC_Stag
) SRC
)
);


-- Component exp_passthru, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_passthru AS
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
SQ_OFAC_Stag.Country as Country,
CURRENT_TIMESTAMP () as current_ts,
SQ_OFAC_Stag.source_record_id
FROM
SQ_OFAC_Stag
);


-- Component OFAC_Stag_hist, Type TARGET 
INSERT INTO OFAC_Stag_hist
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
Country,
Load_dttm
)
SELECT
exp_passthru.FirstName as FirstName,
exp_passthru.LastName as LastName,
exp_passthru.FullName as FullName,
exp_passthru.AddressLine1 as AddressLine1,
exp_passthru.AddressLine2 as AddressLine2,
exp_passthru.AddressLine3 as AddressLine3,
exp_passthru.City as City,
exp_passthru.StateCD as StateCD,
exp_passthru.PostalCode as PostalCode,
exp_passthru.ContactType as ContactType,
exp_passthru.TaxID as TaxID,
exp_passthru.ClaimNumber as ClaimNumber,
exp_passthru.PolicyNumber as PolicyNumber,
exp_passthru.AccountNumber as AccountNumber,
exp_passthru.SourceSystem as SourceSystem,
exp_passthru.UpdateTime as UpdateTime,
exp_passthru.SourceGroup as SourceGroup,
exp_passthru.TaxStatus as TaxStatus,
exp_passthru.Country as Country,
exp_passthru.current_ts as Load_dttm
FROM
exp_passthru;


-- PIPELINE END FOR 1

-- PIPELINE START FOR 2

-- Component SQ_OFAC_Stag_POST_SQL_DUMMY, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_OFAC_Stag_POST_SQL_DUMMY AS
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
SELECT	OFAC_Stag.FirstName, OFAC_Stag.LastName, OFAC_Stag.FullName,

		OFAC_Stag.AddressLine1, OFAC_Stag.AddressLine2, OFAC_Stag.AddressLine3,

		OFAC_Stag.City, OFAC_Stag.StateCD, OFAC_Stag.PostalCode, OFAC_Stag.ContactType,

		OFAC_Stag.TaxID, OFAC_Stag.ClaimNumber, OFAC_Stag.PolicyNumber,

		OFAC_Stag.AccountNumber, OFAC_Stag.SourceSystem, OFAC_Stag.UpdateTime,

		OFAC_Stag.SourceGroup, OFAC_Stag.TaxStatus, OFAC_Stag.Country

FROM

 OFAC_Stag

WHERE	1=2
) SRC
)
);


-- Component OFAC_Stag_hist_POST_SQL, Type TARGET 
INSERT INTO OFAC_Stag_hist
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
SQ_OFAC_Stag_POST_SQL_DUMMY.FirstName as FirstName,
SQ_OFAC_Stag_POST_SQL_DUMMY.LastName as LastName,
SQ_OFAC_Stag_POST_SQL_DUMMY.FullName as FullName,
SQ_OFAC_Stag_POST_SQL_DUMMY.AddressLine1 as AddressLine1,
SQ_OFAC_Stag_POST_SQL_DUMMY.AddressLine2 as AddressLine2,
SQ_OFAC_Stag_POST_SQL_DUMMY.AddressLine3 as AddressLine3,
SQ_OFAC_Stag_POST_SQL_DUMMY.City as City,
SQ_OFAC_Stag_POST_SQL_DUMMY.StateCD as StateCD,
SQ_OFAC_Stag_POST_SQL_DUMMY.PostalCode as PostalCode,
SQ_OFAC_Stag_POST_SQL_DUMMY.ContactType as ContactType,
SQ_OFAC_Stag_POST_SQL_DUMMY.TaxID as TaxID,
SQ_OFAC_Stag_POST_SQL_DUMMY.ClaimNumber as ClaimNumber,
SQ_OFAC_Stag_POST_SQL_DUMMY.PolicyNumber as PolicyNumber,
SQ_OFAC_Stag_POST_SQL_DUMMY.AccountNumber as AccountNumber,
SQ_OFAC_Stag_POST_SQL_DUMMY.SourceSystem as SourceSystem,
SQ_OFAC_Stag_POST_SQL_DUMMY.UpdateTime as UpdateTime,
SQ_OFAC_Stag_POST_SQL_DUMMY.SourceGroup as SourceGroup,
SQ_OFAC_Stag_POST_SQL_DUMMY.TaxStatus as TaxStatus,
SQ_OFAC_Stag_POST_SQL_DUMMY.Country as Country
FROM
SQ_OFAC_Stag_POST_SQL_DUMMY;


-- PIPELINE END FOR 2

END; ';