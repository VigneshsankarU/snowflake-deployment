-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_PA_LIMITS_BY_STATE_EXTRACT("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' BEGIN 

-- Component SQ_PA_Limits_by_states_SRC, Type TABLE_DDL Creating an empty table
CREATE OR REPLACE TEMPORARY TABLE SQ_PA_Limits_by_states
(
BASE_ST varchar(50),
POL_TYPE varchar(50),
Limits varchar(50),
TotalPolicies varchar(50),
TotalPremium varchar(50),
source_record_id number autoincrement start 1 increment 1
);


-- Component SQ_PA_Limits_by_states_SRC, Type IMPORT_DATA Importing Data
;


-- Component EXP_PA_Limits_by_states, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXP_PA_Limits_by_states AS
(
SELECT
SQ_PA_Limits_by_states.BASE_ST as BASE_ST,
SQ_PA_Limits_by_states.POL_TYPE as POL_TYPE,
SQ_PA_Limits_by_states.Limits as Limits,
SQ_PA_Limits_by_states.TotalPolicies as TotalPolicies,
''$'' || SQ_PA_Limits_by_states.TotalPremium as out_TotalPremium,
SQ_PA_Limits_by_states.source_record_id
FROM
SQ_PA_Limits_by_states
);


-- Component PA_Limits_by_states, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE PA_Limits_by_states AS
(
SELECT
EXP_PA_Limits_by_states.BASE_ST as BASE_ST,
EXP_PA_Limits_by_states.POL_TYPE as POL_TYPE,
EXP_PA_Limits_by_states.Limits as Limits,
EXP_PA_Limits_by_states.TotalPolicies as TotalPolicies,
EXP_PA_Limits_by_states.out_TotalPremium as TotalPremium
FROM
EXP_PA_Limits_by_states
);


-- Component PA_Limits_by_states, Type EXPORT_DATA Exporting data
;


END; ';