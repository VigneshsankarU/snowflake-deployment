-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_ALFA_LIFT_035_OFAC_EMPLOYEE("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' BEGIN 

-- Component OFAC_Stag_Employee, Type TRUNCATE_TABLE 
TRUNCATE TABLE TD_OFAC_Stag;


-- PIPELINE START FOR 2

-- Component SQ_OFAC_VENDOR_FILE_SRC, Type TABLE_DDL Creating an empty table
CREATE OR REPLACE TEMPORARY TABLE SQ_OFAC_VENDOR_FILE
(
NAME varchar(50),
ADDRESS varchar(55),
CITY varchar(30),
STATE varchar(15),
ZIP varchar(15),
COUNTRY varchar(20),
VENDORID varchar(12),
source_record_id number autoincrement start 1 increment 1
);


-- Component SQ_OFAC_VENDOR_FILE_SRC, Type IMPORT_DATA Importing Data
;


-- PIPELINE START FOR 1

-- Component SQ_OFAC_EMPLOYEE_FILE1_SRC, Type TABLE_DDL Creating an empty table
CREATE OR REPLACE TEMPORARY TABLE SQ_OFAC_EMPLOYEE_FILE1
(
Name varchar(50),
Address varchar(50),
City varchar(40),
State varchar(15),
Zip varchar(15),
Country varchar(20),
EmployeeID varchar(12),
source_record_id number autoincrement start 1 increment 1
);


-- Component SQ_OFAC_EMPLOYEE_FILE1_SRC, Type IMPORT_DATA Importing Data
;


-- Component EXP_Vendor, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXP_Vendor AS
(
SELECT
REPLACE(REPLACE(SQ_OFAC_VENDOR_FILE.NAME,chr ( 34 ),'' ''),chr ( 39 ),NULL) as v_FullName,
ltrim ( rtrim ( v_FullName ) ) as o_FullName,
REPLACE(REPLACE(SQ_OFAC_VENDOR_FILE.ADDRESS,chr ( 34 ),'' ''),chr ( 39 ),NULL) as v_AddressLine1,
ltrim ( rtrim ( v_AddressLine1 ) ) as o_AddressLine1,
REPLACE(REPLACE(SQ_OFAC_VENDOR_FILE.CITY,chr ( 34 ),'' ''),chr ( 39 ),NULL) as v_City,
ltrim ( rtrim ( v_City ) ) as o_City,
ltrim ( rtrim ( SQ_OFAC_VENDOR_FILE.STATE ) ) as o_State,
ltrim ( rtrim ( SQ_OFAC_VENDOR_FILE.ZIP ) ) as o_Zip,
''CompanyVendor'' as o_ContactType,
decode ( true , ltrim ( rtrim ( SQ_OFAC_VENDOR_FILE.COUNTRY ) ) = ''USA'' , ''United States'' , ltrim ( rtrim ( SQ_OFAC_VENDOR_FILE.COUNTRY ) ) = ''CAN'' , ''Canada'' , ltrim ( rtrim ( SQ_OFAC_VENDOR_FILE.COUNTRY ) ) = ''AUS'' , ''Australia'' , ltrim ( rtrim ( SQ_OFAC_VENDOR_FILE.COUNTRY ) ) ) as o_Country,
SQ_OFAC_VENDOR_FILE.VENDORID as VENDORID,
''VENDOR'' as Source,
''VENDOR'' as o_SourceGroup,
SQ_OFAC_VENDOR_FILE.source_record_id
FROM
SQ_OFAC_VENDOR_FILE
);


-- Component EXP_EMPLOYEE1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXP_EMPLOYEE1 AS
(
SELECT
REPLACE(REPLACE(SQ_OFAC_EMPLOYEE_FILE1.Name,chr ( 34 ),'' ''),chr ( 39 ),NULL) as v_FullName,
ltrim ( rtrim ( v_FullName ) ) as o_FullName,
REPLACE(REPLACE(SQ_OFAC_EMPLOYEE_FILE1.Address,chr ( 34 ),'' ''),chr ( 39 ),NULL) as v_AddressLine1,
ltrim ( rtrim ( v_AddressLine1 ) ) as o_AddressLine1,
REPLACE(REPLACE(SQ_OFAC_EMPLOYEE_FILE1.City,chr ( 34 ),'' ''),chr ( 39 ),NULL) as v_City,
ltrim ( rtrim ( v_City ) ) as o_City,
ltrim ( rtrim ( SQ_OFAC_EMPLOYEE_FILE1.State ) ) as o_State,
ltrim ( rtrim ( SQ_OFAC_EMPLOYEE_FILE1.Zip ) ) as o_Zip,
''Person'' as o_ContactType,
decode ( true , ltrim ( rtrim ( SQ_OFAC_EMPLOYEE_FILE1.Country ) ) = ''USA'' , ''United States'' , ltrim ( rtrim ( SQ_OFAC_EMPLOYEE_FILE1.Country ) ) = ''CAN'' , ''Canada'' , ltrim ( rtrim ( SQ_OFAC_EMPLOYEE_FILE1.Country ) ) = ''AUS'' , ''Australia'' , ltrim ( rtrim ( SQ_OFAC_EMPLOYEE_FILE1.Country ) ) ) as o_Country,
SQ_OFAC_EMPLOYEE_FILE1.EmployeeID as EMPLOYEEID,
''EMPLOYEE'' as Source,
''EMPLOYEE'' as o_SourceGroup,
SQ_OFAC_EMPLOYEE_FILE1.source_record_id
FROM
SQ_OFAC_EMPLOYEE_FILE1
);


-- Component OFAC_Stag_Vendorg, Type TARGET 
INSERT INTO DB_T_PROD_STAG.ofac_stag
(
FullName,
AddressLine1,
City,
StateCD,
PostalCode,
ContactType,
PolicyNumber,
SourceSystem,
SourceGroup,
Country
)
SELECT
EXP_Vendor.o_FullName as FullName,
EXP_Vendor.o_AddressLine1 as AddressLine1,
EXP_Vendor.o_City as City,
EXP_Vendor.o_State as StateCD,
EXP_Vendor.o_Zip as PostalCode,
EXP_Vendor.o_ContactType as ContactType,
EXP_Vendor.VENDORID as PolicyNumber,
EXP_Vendor.Source as SourceSystem,
EXP_Vendor.o_SourceGroup as SourceGroup,
EXP_Vendor.o_Country as Country
FROM
EXP_Vendor;


-- PIPELINE END FOR 2

-- Component OFAC_Stag_Employee, Type TARGET 
INSERT INTO DB_T_PROD_STAG.ofac_Stag
(
FullName,
AddressLine1,
City,
StateCD,
PostalCode,
ContactType,
PolicyNumber,
SourceSystem,
SourceGroup,
Country
)
SELECT
EXP_EMPLOYEE1.o_FullName as FullName,
EXP_EMPLOYEE1.o_AddressLine1 as AddressLine1,
EXP_EMPLOYEE1.o_City as City,
EXP_EMPLOYEE1.o_State as StateCD,
EXP_EMPLOYEE1.o_Zip as PostalCode,
EXP_EMPLOYEE1.o_ContactType as ContactType,
EXP_EMPLOYEE1.EMPLOYEEID as PolicyNumber,
EXP_EMPLOYEE1.Source as SourceSystem,
EXP_EMPLOYEE1.o_SourceGroup as SourceGroup,
EXP_EMPLOYEE1.o_Country as Country
FROM
EXP_EMPLOYEE1;


-- PIPELINE END FOR 1

END; ';