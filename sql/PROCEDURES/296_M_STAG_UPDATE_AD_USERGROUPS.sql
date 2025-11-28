-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_STAG_UPDATE_AD_USERGROUPS("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' BEGIN 

-- Component AD_USERGROUPS1, Type TRUNCATE_TABLE 
TRUNCATE TABLE DB_T_STAG_MEMBXREF_PROD.AD_USERGROUPS;


-- Component SQ_AD_UserGroups, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_AD_UserGroups AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as User_Name,
$2 as Group_Name,
$3 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT UPPER(SQLADUSERS.User_Name) AS USER_NAME
	, UPPER(SQLADUSERGROUPS.Group_Name) AS GROUP_NAME
FROM DB_T_STAG_MEMBXREF_PROD.AD_USERS SQLADUSERS
INNER JOIN DB_T_STAG_MEMBXREF_PROD.AD_USERGROUPS SQLADUSERGROUPS 
	ON SQLADUSERGROUPS.User_Name = SQLADUSERS.User_Name
--WHERE SQLADUSERS."Account_Disabled" = ''False''
) SRC
)
);


-- Component exp_update_ad_usergroups, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_update_ad_usergroups AS
(
SELECT
SQ_AD_UserGroups.User_Name as User_Name,
SQ_AD_UserGroups.Group_Name as Group_Name,
SQ_AD_UserGroups.source_record_id
FROM
SQ_AD_UserGroups
);


-- Component AD_USERGROUPS1, Type TARGET 
INSERT INTO DB_T_STAG_MEMBXREF_PROD.AD_USERGROUPS
(
USER_NAME,
GROUP_NAME
)
SELECT
exp_update_ad_usergroups.User_Name as USER_NAME,
exp_update_ad_usergroups.Group_Name as GROUP_NAME
FROM
exp_update_ad_usergroups;


END; ';