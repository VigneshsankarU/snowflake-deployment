-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_COMMERCIAL_AUTO_LIMITS_EXTRACT("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' BEGIN 

-- Component SQ_Commerical_Auto_BI_Limits_SRC, Type TABLE_DDL Creating an empty table
CREATE OR REPLACE TEMPORARY TABLE SQ_Commerical_Auto_BI_Limits
(
DwellingLimits varchar(100),
TotalPolicies varchar(50),
TotalPremium varchar(50),
Bucket integer,
source_record_id number autoincrement start 1 increment 1
);


-- Component SQ_Commerical_Auto_BI_Limits_SRC, Type IMPORT_DATA Importing Data
;


-- Component Exp_trans, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE Exp_trans AS
(
SELECT
SQ_Commerical_Auto_BI_Limits.DwellingLimits as DwellingLimits,
SQ_Commerical_Auto_BI_Limits.TotalPolicies as TotalPolicies,
SQ_Commerical_Auto_BI_Limits.TotalPremium as TotalPremium,
SQ_Commerical_Auto_BI_Limits.source_record_id
FROM
SQ_Commerical_Auto_BI_Limits
);


-- Component Commercial_Auto_Limits, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE Commercial_Auto_Limits AS
(
SELECT
Exp_trans.DwellingLimits as Limits,
Exp_trans.TotalPolicies as TotalPolicies,
Exp_trans.TotalPremium as TotalPremium
FROM
Exp_trans
);


-- Component Commercial_Auto_Limits, Type EXPORT_DATA Exporting data
;


END; ';