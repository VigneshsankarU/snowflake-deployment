-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_STAG_UPDATE_AD_USERS("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' BEGIN 

-- Component AD_USERS1, Type TRUNCATE_TABLE 
TRUNCATE TABLE DB_T_STAG_MEMBXREF_PROD.AD_USERS;


-- Component SQ_AD_Users, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_AD_Users AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as User_Name,
$2 as Agent_Number,
$3 as First_Name,
$4 as Last_Name,
$5 as Full_Name,
$6 as Title,
$7 as User_Type,
$8 as Mail,
$9 as Street_Address,
$10 as City,
$11 as State_or_Province,
$12 as Postal_Code,
$13 as Mobile,
$14 as Telephone_Number,
$15 as Region,
$16 as District,
$17 as Service_Center_Number,
$18 as IMG_PHOTO,
$19 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT UPPER(SQLADUSERS.User_Name) AS USER_NAME
	, UPPER(SQLADUSERS.Agent_Number) AS AGENT_NUMBER
	, UPPER(SQLADUSERS.First_Name) AS FIRST_NAME
	, UPPER(SQLADUSERS.Last_Name) AS LAST_NAME
	, UPPER(SQLADUSERS.Full_Name) AS FULL_NAME
	, UPPER(SQLADUSERS.ad_Title) AS TITLE
	, CASE 
		WHEN SQLADUSERS.ad_Title LIKE ''AGENT'' THEN ''A''
		WHEN SQLADUSERS.ad_Title LIKE ''IE AGENT'' THEN ''A''
		WHEN SQLADUSERS.ad_Title LIKE ''IE Financing Agent'' THEN ''A''
		WHEN SQLADUSERS.ad_Title LIKE ''Financing Agent'' THEN ''A''
		WHEN SQLADUSERS.ad_Title LIKE ''Associate Agent'' THEN ''A''
		WHEN SQLADUSERS.ad_Title LIKE ''GA Associate Agent'' THEN ''A''
		WHEN SQLADUSERS.ad_Title LIKE ''GA Indy Agent'' THEN ''A''
		WHEN SQLADUSERS.ad_Title LIKE ''MS Associate Agent'' THEN ''A''
/* EIM_46962 */
		WHEN SQLADUSERS.ad_Title LIKE ''IE AGENT ALABAMA'' THEN ''A''
		WHEN SQLADUSERS.ad_Title LIKE ''AL ASSOCIATE AGENT'' THEN ''A''
		WHEN SQLADUSERS.ad_Title LIKE ''AL IE ASSOCIATE'' THEN ''A''
		WHEN SQLADUSERS.ad_Title LIKE ''AL-IND CSR'' THEN ''C''
		WHEN SQLADUSERS.ad_Title LIKE ''SALES REPRESENTATIVE'' THEN ''C''
		WHEN SQLADUSERS.ad_Title LIKE ''SALES SPECIALIST'' THEN ''C''
		WHEN SQLADUSERS.ad_Title LIKE ''SALES MULTI-LINE SPECIALIST'' THEN ''C''
		WHEN SQLADUSERS.ad_Title LIKE ''ASPIRING AGENT'' THEN ''AA''
		WHEN SQLADUSERS.ad_Title LIKE ''MS - FIELD SALES SPECIALIST I'' THEN ''A''
		
		WHEN SQLADUSERS.ad_Title LIKE ''CSR I'' THEN ''C''
		WHEN SQLADUSERS.ad_Title LIKE ''CSR II'' THEN ''C''
		WHEN SQLADUSERS.ad_Title LIKE ''CSR III'' THEN ''C''
		WHEN SQLADUSERS.ad_Title LIKE ''MS-IND CSR'' THEN ''C''
		WHEN SQLADUSERS.ad_Title LIKE ''GA-IND CSR'' THEN ''C''
		WHEN SQLADUSERS.ad_Title LIKE ''GA CSR'' THEN ''C''
		WHEN SQLADUSERS.ad_Title LIKE ''Mgr Dist% Sales O/S'' THEN ''DM''
		WHEN SQLADUSERS.ad_Title LIKE ''Mgr Dist% Sales'' THEN ''DM''
WHEN SQLADUSERS.ad_Title LIKE ''MGR DISTRICT SALES 2023+'' THEN ''DM''  /* EIM-47466 */
		WHEN SQLADUSERS.ad_Title LIKE ''Spvr Dist O/S'' THEN ''DS''
		WHEN SQLADUSERS.ad_Title LIKE ''Spvr Dist'' THEN ''DS''
		WHEN SQLADUSERS.ad_Title LIKE ''SVP, Marketing - GA/MS'' THEN ''RM''
		WHEN SQLADUSERS.ad_Title LIKE ''VP Marketing - Alabama'' THEN ''RM''
		WHEN SQLADUSERS.ad_Title LIKE ''VP Marketing GA/MS'' THEN ''RM''
		WHEN SQLADUSERS.ad_Title LIKE ''CSR I - Claims'' THEN ''C''
		WHEN SQLADUSERS.ad_Title LIKE ''CSR II - Claims'' THEN ''C''
		WHEN SQLADUSERS.ad_Title LIKE ''CSR III - Claims'' THEN ''C''
		ELSE '''' 
		END AS USER_TYPE
	, UPPER(SQLADUSERS.eMail) AS MAIL
	, UPPER(SQLADUSERS.Street_Address) AS STREET_ADDRESS
	, UPPER(SQLADUSERS.City) AS CITY
	, UPPER(SQLADUSERS.State) AS STATE_OR_PROVINCE
	, UPPER(SQLADUSERS.zip) AS POSTAL_CODE
	, UPPER(SQLADUSERS.Mobile_number) AS MOBILE
	, UPPER(SQLADUSERS.Telephone_Number) AS TELEPHONE_NUMBER
	, UPPER(SQLADUSERS.Region) AS REGION
	, UPPER(SQLADUSERS.District) AS DISTRICT
/*
/* 	, UPPER(SQLADUSERS.Extension_Attribute_11) AS SERVICE_CENTER_NUMBER */
/*  XXX - USE OFFICE FOR SERVICE CENTER NUMBER WHEN THE TITLE IE EITHER MS-IND CSR OR GA-IND CSR */
/*  USE OFFICE IF IT MATCHES THE PATTERN SC%, ELSE USE EXTENSION_ATTRIBUTE_11 
,case		WHEN SQLADUSERS.ad_Title LIKE ''MS-IND CSR'' THEN REPLACE(service_center_number, ''SC'', '''') 
		WHEN SQLADUSERS.ad_Title LIKE ''GA-IND CSR'' THEN REPLACE(service_center_number, ''SC'', '''') end
*/
	, CASE
		WHEN SQLADUSERS.service_center_number LIKE ''SC[0-9]%'' THEN CAST(REPLACE(service_center_number, ''SC'', '''') AS INT)
--		WHEN SQLADUSERS.Extension_Attribute_11 LIKE ''SC%'' THEN CAST(REPLACE(Extension_Attribute_11, ''SC'', '''') AS INT)
--		WHEN ISNUMERIC(SQLADUSERS.Extension_Attribute_11) = 1 THEN CAST(SQLADUSERS.Extension_Attribute_11 AS INT)
		ELSE ''''
		END AS SERVICE_CENTER_NUMBER
	, UPPER(SQLADUSERS.IMG_PHOTO) AS IMG_PHOTO
--select *
FROM DB_T_STAG_MEMBXREF_PROD.AD_USERS SQLADUSERS
--WHERE SQLADUSERS.Account_Disabled = ''False''
) SRC
)
);


-- Component exp_update_ad_users, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_update_ad_users AS
(
SELECT
SQ_AD_Users.User_Name as User_Name,
SQ_AD_Users.Agent_Number as Agent_Number,
SQ_AD_Users.First_Name as First_Name,
SQ_AD_Users.Last_Name as Last_Name,
SQ_AD_Users.Full_Name as Full_Name,
SQ_AD_Users.Title as AD_Title,
SQ_AD_Users.User_Type as User_Type,
SQ_AD_Users.Mail as Email,
SQ_AD_Users.Street_Address as Street_Address,
SQ_AD_Users.City as City,
SQ_AD_Users.State_or_Province as State,
SQ_AD_Users.Postal_Code as ZIP,
SQ_AD_Users.Mobile as Mobile_Number,
SQ_AD_Users.Telephone_Number as Telephone_Number,
SQ_AD_Users.Region as Region,
SQ_AD_Users.District as District,
SQ_AD_Users.Service_Center_Number as Service_Center_Number,
SQ_AD_Users.IMG_PHOTO as IMG_PHOTO,
SQ_AD_Users.source_record_id
FROM
SQ_AD_Users
);


-- Component AD_USERS1, Type TARGET 
INSERT INTO DB_T_STAG_MEMBXREF_PROD.AD_USERS
(
USER_NAME,
AGENT_NUMBER,
FIRST_NAME,
LAST_NAME,
FULL_NAME,
AD_TITLE,
USER_TYPE,
EMAIL,
STREET_ADDRESS,
CITY,
STATE,
ZIP,
MOBILE_NUMBER,
TELEPHONE_NUMBER,
REGION,
DISTRICT,
SERVICE_CENTER_NUMBER,
IMG_PHOTO
)
SELECT
exp_update_ad_users.User_Name as USER_NAME,
exp_update_ad_users.Agent_Number as AGENT_NUMBER,
exp_update_ad_users.First_Name as FIRST_NAME,
exp_update_ad_users.Last_Name as LAST_NAME,
exp_update_ad_users.Full_Name as FULL_NAME,
exp_update_ad_users.AD_Title as AD_TITLE,
exp_update_ad_users.User_Type as USER_TYPE,
exp_update_ad_users.Email as EMAIL,
exp_update_ad_users.Street_Address as STREET_ADDRESS,
exp_update_ad_users.City as CITY,
exp_update_ad_users.State as STATE,
exp_update_ad_users.ZIP as ZIP,
exp_update_ad_users.Mobile_Number as MOBILE_NUMBER,
exp_update_ad_users.Telephone_Number as TELEPHONE_NUMBER,
exp_update_ad_users.Region as REGION,
exp_update_ad_users.District as DISTRICT,
exp_update_ad_users.Service_Center_Number as SERVICE_CENTER_NUMBER,
exp_update_ad_users.IMG_PHOTO as IMG_PHOTO
FROM
exp_update_ad_users;


END; ';